#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_path.hpp"
#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_table_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
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

DeltaClassicSchemaEntry &DeltaClassicCatalog::AddSchema(const string &name, const string &schema_path) {
	auto existing = schemas.find(name);
	if (existing != schemas.end()) {
		// Case-insensitive collision (e.g. "Sales" and "sales" on S3): the first one listed wins
		return *existing->second;
	}
	CreateSchemaInfo info;
	info.SetQualifiedName(QualifiedName({Identifier(name)}, Identifier()));
	auto schema = make_uniq<DeltaClassicSchemaEntry>(*this, info, schema_path);
	auto &result = *schema;
	schemas[name] = std::move(schema);
	return result;
}

static vector<pair<string, string>> TableEntries(const string &parent_path, const set<string> &names) {
	vector<pair<string, string>> result;
	for (auto &name : names) {
		result.emplace_back(name, DeltaClassicJoinPath(parent_path, name));
	}
	return result;
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
	auto listing = DeltaClassicListDirectory(fs, base_path);

	if (listing.is_delta_table) {
		// The attached path is itself a Delta table: expose it as main.<last path component>
		layout = DeltaClassicLayout::SINGLE_TABLE;
		auto &schema = AddSchema(DEFAULT_SCHEMA, base_path);
		schema.tables.SetEntries({{DeltaClassicLastComponent(base_path), base_path}});
	} else if (!listing.tables.empty()) {
		// Single-schema mode: the Delta tables are direct children.
		// Use DEFAULT_SCHEMA ("main") so unqualified table names work after USE db
		layout = DeltaClassicLayout::SINGLE_SCHEMA;
		auto &schema = AddSchema(DEFAULT_SCHEMA, base_path);
		schema.tables.SetEntries(TableEntries(base_path, listing.tables));
	} else {
		// Multi-schema mode: each child directory is a schema
		layout = listing.directories.empty() ? DeltaClassicLayout::EMPTY : DeltaClassicLayout::MULTI_SCHEMA;
		for (auto &dir_name : listing.directories) {
			auto schema_path = DeltaClassicJoinPath(base_path, dir_name);
			auto &schema = AddSchema(dir_name, schema_path);
			if (listing.recursive) {
				// A recursive listing already saw every table, so don't list each schema again
				schema.tables.SetEntries(TableEntries(schema_path, listing.nested_tables[dir_name]));
			}
		}
		// Ensure a "main" schema exists so USE db works
		if (schemas.find(DEFAULT_SCHEMA) == schemas.end()) {
			auto &main_schema = AddSchema(DEFAULT_SCHEMA, base_path);
			main_schema.tables.SetEntries({});
		}
	}

	schemas_loaded = true;
}

optional_ptr<CatalogEntry> DeltaClassicCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException(UnsupportedWriteMessage("CREATE SCHEMA"));
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
	for (auto &schema : result) {
		callback(schema.get());
	}
}

optional_ptr<SchemaCatalogEntry> DeltaClassicCatalog::LookupSchema(CatalogTransaction transaction,
                                                                   const EntryLookupInfo &schema_lookup,
                                                                   OnEntryNotFound if_not_found) {
	if (transaction.HasContext()) {
		DiscoverSchemas(transaction.GetContext());
	}

	auto &schema_name = schema_lookup.GetEntryName();

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

	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw BinderException("Schema \"%s\" not found in delta_classic catalog \"%s\"", schema_name, GetName());
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
	throw NotImplementedException(
	    "delta_classic does not support CREATE TABLE AS SELECT: create the table, then INSERT into it");
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
