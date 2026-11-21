#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_path.hpp"
#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_table_entry.hpp"
#include "storage/delta_classic_table_set.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

static idx_t NextCatalogId() {
	static atomic<idx_t> next_catalog_id {0};
	return next_catalog_id++;
}

DeltaClassicCatalog::DeltaClassicCatalog(AttachedDatabase &db, const string &base_path,
                                         unordered_map<string, Value> delta_options, bool writable)
    : Catalog(db), base_path(base_path), delta_options(std::move(delta_options)), writable(writable),
      layout(DeltaClassicLayout::EMPTY), schemas_loaded(false), catalog_id(NextCatalogId()) {
}

DeltaClassicCatalog::~DeltaClassicCatalog() = default;

void DeltaClassicCatalog::Initialize(bool load_builtin) {
}

string DeltaClassicCatalog::GetCatalogType() {
	return "delta_classic";
}

DeltaClassicSchemaEntry &DeltaClassicCatalog::AddSchema(const string &name, const string &schema_path,
                                                        DeltaClassicSchemaKind kind,
                                                        optional_ptr<const DeltaClassicListing> listing) {
	auto existing = schemas.find(name);
	if (existing != schemas.end()) {
		// Case-insensitive collision (e.g. "Sales" and "sales" on S3): the first one listed wins
		return *existing->second;
	}
	CreateSchemaInfo info;
	info.SetQualifiedName(QualifiedName({Identifier(name)}, Identifier()));
	auto schema = make_uniq<DeltaClassicSchemaEntry>(*this, info, schema_path, kind, nullptr);
	if (listing) {
		schema->ApplyListing(*listing);
	}
	auto &result = *schema;
	schemas[name] = std::move(schema);
	return result;
}

//! The listing of a child directory: taken from a recursive listing, or listed now
static DeltaClassicListing ListChild(FileSystem &fs, const string &parent_path, const DeltaClassicListing &parent,
                                     const string &child) {
	if (parent.recursive) {
		auto entry = parent.children.find(child);
		return entry == parent.children.end() ? DeltaClassicListing() : entry->second;
	}
	return DeltaClassicListDirectory(fs, DeltaClassicJoinPath(parent_path, child));
}

static bool IsLakehouse(const string &path, const DeltaClassicListing &listing) {
	if (!DeltaClassicLakehouseName(DeltaClassicLastComponent(path)).empty()) {
		return true;
	}
	// A lakehouse addressed by its id (abfss://<workspace id>@onelake.dfs.fabric.microsoft.com/<lakehouse id>)
	auto &dirs = listing.directories;
	return dirs.find(DELTA_CLASSIC_LAKEHOUSE_TABLES) != dirs.end() && dirs.find("Files") != dirs.end();
}

static bool HasLakehouseItems(const DeltaClassicListing &listing) {
	for (auto &directory : listing.directories) {
		if (!DeltaClassicLakehouseName(directory).empty()) {
			return true;
		}
	}
	return false;
}

void DeltaClassicCatalog::AddLakehouseSchemas(ClientContext &context, const string &lakehouse_path,
                                              const DeltaClassicListing &listing) {
	// The schemas of a lakehouse are the directories in its Tables directory. Tables directly in Tables (a lakehouse
	// without schemas) are left out.
	auto &fs = FileSystem::GetFileSystem(context);
	auto tables_path = DeltaClassicJoinPath(lakehouse_path, DELTA_CLASSIC_LAKEHOUSE_TABLES);
	auto tables_listing = ListChild(fs, lakehouse_path, listing, DELTA_CLASSIC_LAKEHOUSE_TABLES);
	for (auto &directory : tables_listing.directories) {
		optional_ptr<const DeltaClassicListing> schema_listing;
		auto entry = tables_listing.children.find(directory);
		if (tables_listing.recursive && entry != tables_listing.children.end()) {
			schema_listing = &entry->second;
		}
		AddSchema(directory, DeltaClassicJoinPath(tables_path, directory), DeltaClassicSchemaKind::LAKEHOUSE_SCHEMA,
		          schema_listing);
	}
}

void DeltaClassicCatalog::AddWorkspaceItems(ClientContext &context, const DeltaClassicListing &listing) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &directory : listing.directories) {
		auto lakehouse = DeltaClassicLakehouseName(directory);
		if (lakehouse.empty()) {
			// Warehouses, semantic models, notebooks, ...
			continue;
		}
		auto tables_path =
		    DeltaClassicJoinPath(DeltaClassicJoinPath(base_path, directory), DELTA_CLASSIC_LAKEHOUSE_TABLES);
		if (listing.recursive) {
			auto item = listing.children.find(directory);
			if (item == listing.children.end()) {
				continue;
			}
			auto tables = item->second.children.find(DELTA_CLASSIC_LAKEHOUSE_TABLES);
			if (tables == item->second.children.end()) {
				continue;
			}
			AddSchema(lakehouse, tables_path, DeltaClassicSchemaKind::LAKEHOUSE, &tables->second);
			continue;
		}
		// Also guards against a listing that mangled the name
		if (!fs.DirectoryExists(tables_path)) {
			continue;
		}
		AddSchema(lakehouse, tables_path, DeltaClassicSchemaKind::LAKEHOUSE, nullptr);
	}
}

void DeltaClassicCatalog::DiscoverSchemas(ClientContext &context) {
	if (schemas_loaded) {
		return;
	}
	lock_guard<mutex> lock(schema_lock);
	if (schemas_loaded) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	if (DeltaClassicIsOneLakeWorkspace(base_path)) {
		// A OneLake workspace. Listing it can fail: OneLake leaves fields out of the workspace's List Paths response
		// that the Azure SDK requires (duckdb/duckdb-azure#226). Lakehouses are then found by name, on lookup.
		layout = DeltaClassicLayout::WORKSPACE;
		DeltaClassicListing listing;
		try {
			listing = DeltaClassicListDirectory(fs, base_path);
		} catch (std::exception &) {
			listing = DeltaClassicListing();
		}
		AddWorkspaceItems(context, listing);
	} else {
		auto listing = DeltaClassicListDirectory(fs, base_path);
		if (listing.is_delta_table) {
			// The attached path is itself a Delta table: expose it as main.<last path component>
			layout = DeltaClassicLayout::SINGLE_TABLE;
			auto &schema = AddSchema(DEFAULT_SCHEMA, base_path, DeltaClassicSchemaKind::PLACEHOLDER, nullptr);
			schema.SetListedTables({{DeltaClassicLastComponent(base_path), base_path}});
		} else if (IsLakehouse(base_path, listing)) {
			// A Fabric lakehouse: <db>.<schema>.<table>
			layout = DeltaClassicLayout::LAKEHOUSE;
			AddLakehouseSchemas(context, base_path, listing);
		} else if (HasLakehouseItems(listing)) {
			// A directory of lakehouses, like a OneLake workspace: <db>.<lakehouse>.<schema>.<table>
			layout = DeltaClassicLayout::WORKSPACE;
			AddWorkspaceItems(context, listing);
		} else if (!listing.tables.empty()) {
			// Single-schema mode: the Delta tables are direct children, other directories nested schemas of main.
			// Use DEFAULT_SCHEMA ("main") so unqualified table names work after USE db
			layout = DeltaClassicLayout::SINGLE_SCHEMA;
			AddSchema(DEFAULT_SCHEMA, base_path, DeltaClassicSchemaKind::DIRECTORY, &listing);
		} else if (listing.directories.empty()) {
			// Nothing yet: CREATE TABLE goes to main, CREATE SCHEMA makes it a multi schema
			layout = DeltaClassicLayout::EMPTY;
			AddSchema(DEFAULT_SCHEMA, base_path, DeltaClassicSchemaKind::DIRECTORY, &listing);
		} else {
			// Multi-schema mode: each child directory is a schema, and each directory below it a nested schema
			layout = DeltaClassicLayout::MULTI_SCHEMA;
			for (auto &dir_name : listing.directories) {
				optional_ptr<const DeltaClassicListing> schema_listing;
				auto entry = listing.children.find(dir_name);
				if (listing.recursive && entry != listing.children.end()) {
					// A recursive listing already saw every table, so don't list each schema again
					schema_listing = &entry->second;
				}
				AddSchema(dir_name, DeltaClassicJoinPath(base_path, dir_name), DeltaClassicSchemaKind::DIRECTORY,
				          schema_listing);
			}
		}
	}
	// Ensure a "main" schema exists so USE db works
	if (schemas.find(DEFAULT_SCHEMA) == schemas.end()) {
		AddSchema(DEFAULT_SCHEMA, base_path, DeltaClassicSchemaKind::PLACEHOLDER, nullptr);
	}

	schemas_loaded = true;
}

//! Whether a name is a Fabric item id (a GUID: 8-4-4-4-12 hex digits)
static bool LooksLikeId(const string &name) {
	if (name.size() != 36) {
		return false;
	}
	for (idx_t i = 0; i < name.size(); i++) {
		auto c = name[i];
		bool dash = i == 8 || i == 13 || i == 18 || i == 23;
		if (dash ? c != '-' : !StringUtil::CharacterIsHex(c)) {
			return false;
		}
	}
	return true;
}

optional_ptr<DeltaClassicSchemaEntry> DeltaClassicCatalog::ProbeLakehouse(ClientContext &context, const string &name) {
	{
		lock_guard<mutex> lock(schema_lock);
		if (missing_lakehouses.find(name) != missing_lakehouses.end()) {
			return nullptr;
		}
	}
	auto &fs = FileSystem::GetFileSystem(context);
	string tables_path;
	// By name (<name>.Lakehouse) or by id (<lakehouse id>), the likelier one first
	vector<string> items {name + ".Lakehouse", name};
	if (LooksLikeId(name)) {
		std::swap(items[0], items[1]);
	}
	for (auto &item : items) {
		auto candidate = DeltaClassicJoinPath(DeltaClassicJoinPath(base_path, item), DELTA_CLASSIC_LAKEHOUSE_TABLES);
		try {
			if (fs.DirectoryExists(candidate)) {
				tables_path = candidate;
				break;
			}
		} catch (std::exception &) {
			// OneLake answers an item that doesn't exist with an error, not always with a 404
		}
	}
	lock_guard<mutex> lock(schema_lock);
	if (tables_path.empty()) {
		missing_lakehouses.insert(name);
		return nullptr;
	}
	return &AddSchema(name, tables_path, DeltaClassicSchemaKind::LAKEHOUSE, nullptr);
}

optional_ptr<CatalogEntry> DeltaClassicCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (!writable) {
		throw BinderException(UnsupportedWriteMessage("CREATE SCHEMA"));
	}
	if (!transaction.HasContext()) {
		throw InternalException("delta_classic: CREATE SCHEMA without a client context");
	}
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("delta_classic does not support CREATE OR REPLACE SCHEMA");
	}
	auto &context = transaction.GetContext();
	DiscoverSchemas(context);
	auto name = info.SchemaName().GetIdentifierName();
	auto parents = info.ParentSchemas();
	if (parents.empty()) {
		return CreateTopLevelSchema(context, name, info.on_conflict);
	}
	auto parent = GetSchema(transaction, parents, OnEntryNotFound::THROW_EXCEPTION);
	return parent->Cast<DeltaClassicSchemaEntry>().CreateChildSchema(context, name, info.on_conflict);
}

optional_ptr<CatalogEntry> DeltaClassicCatalog::CreateTopLevelSchema(ClientContext &context, const string &name,
                                                                     OnCreateConflict on_conflict) {
	string path;
	auto kind = DeltaClassicSchemaKind::DIRECTORY;
	switch (layout.load()) {
	case DeltaClassicLayout::SINGLE_TABLE:
		throw BinderException("delta_classic: \"%s\" is attached as a single Delta table, so it can't hold schemas",
		                      GetName());
	case DeltaClassicLayout::SINGLE_SCHEMA:
		throw BinderException("delta_classic: \"%s\" holds its tables in main, so create the schema in it: CREATE "
		                      "SCHEMA %s.main.%s",
		                      GetName(), GetName(), name);
	case DeltaClassicLayout::WORKSPACE:
		throw BinderException("delta_classic: \"%s\" is a workspace, whose schemas are its lakehouses: create the "
		                      "lakehouse in Fabric, or the schema in a lakehouse (CREATE SCHEMA %s.<lakehouse>.%s)",
		                      GetName(), GetName(), name);
	case DeltaClassicLayout::LAKEHOUSE:
		path = DeltaClassicJoinPath(DeltaClassicJoinPath(base_path, DELTA_CLASSIC_LAKEHOUSE_TABLES), name);
		kind = DeltaClassicSchemaKind::LAKEHOUSE_SCHEMA;
		break;
	default:
		path = DeltaClassicJoinPath(base_path, name);
		break;
	}
	{
		lock_guard<mutex> lock(schema_lock);
		if (schemas.find(name) != schemas.end()) {
			if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return nullptr;
			}
			throw CatalogException::EntryAlreadyExists(CatalogType::SCHEMA_ENTRY, Identifier(name));
		}
	}
	DeltaClassicCreateDirectory(context, path);
	lock_guard<mutex> lock(schema_lock);
	auto expected = DeltaClassicLayout::EMPTY;
	if (layout.compare_exchange_strong(expected, DeltaClassicLayout::MULTI_SCHEMA)) {
		// The first schema of an empty database: from now on each directory is a schema, and main holds nothing
		auto main_schema = schemas.find(DEFAULT_SCHEMA);
		if (main_schema != schemas.end()) {
			main_schema->second->kind = DeltaClassicSchemaKind::PLACEHOLDER;
		}
	}
	DeltaClassicListing empty;
	return &AddSchema(name, path, kind, &empty);
}

void DeltaClassicCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	DiscoverSchemas(context);
	vector<reference<SchemaCatalogEntry>> result;
	{
		lock_guard<mutex> lock(schema_lock);
		for (auto &entry : schemas) {
			result.push_back(*entry.second);
		}
	}
	auto transaction = GetCatalogTransaction(context);
	for (auto &schema : result) {
		// Nested schemas too, parents first
		schema.get().ScanSchemaTree(transaction, callback);
	}
}

optional_ptr<SchemaCatalogEntry> DeltaClassicCatalog::LookupSchema(CatalogTransaction transaction,
                                                                   const EntryLookupInfo &schema_lookup,
                                                                   OnEntryNotFound if_not_found) {
	if (transaction.HasContext()) {
		DiscoverSchemas(transaction.GetContext());
	}
	// The lookup carries the whole (possibly nested) schema path: find the top-level schema here, and let the base
	// catalog walk down from it through each schema's LookupEntry(SCHEMA_ENTRY)
	return LookupSchemaPath(transaction, schema_lookup, if_not_found,
	                        [&](const EntryLookupInfo &root) -> optional_ptr<CatalogEntry> {
		                        auto schema = LookupTopLevelSchema(transaction, root.GetEntryName());
		                        return schema ? schema.get() : nullptr;
	                        });
}

optional_ptr<DeltaClassicSchemaEntry> DeltaClassicCatalog::LookupTopLevelSchema(CatalogTransaction transaction,
                                                                                const string &schema_name) {
	{
		lock_guard<mutex> lock(schema_lock);

		// Try exact match
		auto it = schemas.find(schema_name);
		if (it != schemas.end()) {
			return it->second.get();
		}

		// If there's only one schema, return it for DEFAULT_SCHEMA or catalog name lookups
		if (schemas.size() == 1) {
			if (schema_name == DEFAULT_SCHEMA || schema_name == GetName()) {
				return schemas.begin()->second.get();
			}
		}
	}

	if (layout == DeltaClassicLayout::WORKSPACE && transaction.HasContext() && !schema_name.empty()) {
		return ProbeLakehouse(transaction.GetContext(), schema_name);
	}
	return nullptr;
}

ErrorData DeltaClassicCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	// PARTITIONED BY and WITH (table properties) are handled by the Delta extension
	if (!info.Base().sort_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG, "SORTED BY is not supported for tables in a delta_classic catalog");
	}
	return ErrorData();
}

PhysicalOperator &DeltaClassicCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                         LogicalCreateTable &op, PhysicalOperator &plan) {
	if (!writable) {
		throw NotImplementedException("delta_classic databases are read-only");
	}
	auto &schema = op.info->schema.Cast<DeltaClassicSchemaEntry>();
	auto &base = op.info->Base();
	// DuckDB plans a CREATE TABLE AS only when the table doesn't exist, so the name can be claimed
	auto entry = schema.ClaimTable(context, base);
	if (!entry) {
		throw InternalException("delta_classic: CREATE TABLE AS planned for the existing table \"%s\"",
		                        base.GetTableName().GetIdentifierName());
	}
	try {
		// The Delta extension writes the table, as a CREATE TABLE AS of its single table. Only this catalog is
		// registered as modified, so the Delta transaction is still flagged read-only.
		auto &inner_catalog = entry->GetAttachedCatalog(context);
		auto &inner_transaction = Transaction::Get(context, inner_catalog);
		if (inner_transaction.IsReadOnly()) {
			inner_transaction.SetReadWrite();
		}
		return inner_catalog.PlanCreateTableAs(context, planner, op, plan);
	} catch (std::exception &) {
		schema.tables.RemoveTable(*entry);
		throw;
	}
}

PhysicalOperator &DeltaClassicCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	if (!writable) {
		throw NotImplementedException("delta_classic databases are read-only");
	}
	auto &inner_catalog = op.table.Cast<DeltaClassicTableEntry>().GetInnerCatalog();
	// Only this catalog is registered as modified, so the Delta transaction is still flagged read-only. It commits
	// with the rest of the transaction.
	auto &inner_transaction = Transaction::Get(context, inner_catalog);
	if (inner_transaction.IsReadOnly()) {
		inner_transaction.SetReadWrite();
	}
	// The table is attached in child catalog mode, so the Delta extension resolves its own entry for the insert
	return inner_catalog.PlanInsert(context, planner, op, plan);
}

PhysicalOperator &DeltaClassicCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException(UnsupportedWriteMessage("DELETE"));
}

PhysicalOperator &DeltaClassicCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException(UnsupportedWriteMessage("UPDATE"));
}

unique_ptr<LogicalOperator> DeltaClassicCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                                 TableCatalogEntry &table,
                                                                 unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException(UnsupportedWriteMessage("CREATE INDEX"));
}

DatabaseSize DeltaClassicCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

bool DeltaClassicCatalog::InMemory() {
	return false;
}

string DeltaClassicCatalog::GetDBPath() {
	return base_path;
}

void DeltaClassicCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException(UnsupportedWriteMessage("DROP SCHEMA"));
}

string DeltaClassicCatalog::UnsupportedWriteMessage(const string &operation) const {
	if (!writable) {
		return "delta_classic databases are read-only";
	}
	return "delta_classic does not support " + operation;
}

string DeltaClassicCatalog::CreateInternalDbName() {
	lock_guard<mutex> lock(internal_db_lock);
	auto name = "__dc_" + to_string(catalog_id) + "_" + to_string(next_internal_db++);
	internal_db_names.push_back(name);
	return name;
}

void DeltaClassicCatalog::OnDetach(ClientContext &context) {
	vector<string> names;
	{
		lock_guard<mutex> lock(internal_db_lock);
		names = std::move(internal_db_names);
		internal_db_names.clear();
	}
	auto &db_manager = DatabaseManager::Get(context);
	for (auto &name : names) {
		try {
			db_manager.DetachDatabase(context, Identifier(name), OnEntryNotFound::RETURN_NULL, true);
		} catch (std::exception &) {
			// Best effort: the outer catalog is going away either way
		}
	}
}

} // namespace duckdb
