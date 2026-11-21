#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_path.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

DeltaClassicSchemaEntry::DeltaClassicSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const string &schema_path,
                                                 DeltaClassicSchemaKind kind, optional_ptr<SchemaCatalogEntry> parent)
    : SchemaCatalogEntry(catalog, info, parent), schema_path(schema_path), kind(kind), tables(*this), listed(false) {
}

bool DeltaClassicSchemaEntry::HoldsTables() const {
	switch (kind.load()) {
	case DeltaClassicSchemaKind::DIRECTORY:
	case DeltaClassicSchemaKind::LAKEHOUSE_SCHEMA:
		return true;
	default:
		return false;
	}
}

bool DeltaClassicSchemaEntry::ChildKind(DeltaClassicSchemaKind &result) const {
	switch (kind.load()) {
	case DeltaClassicSchemaKind::DIRECTORY:
		result = DeltaClassicSchemaKind::DIRECTORY;
		return true;
	case DeltaClassicSchemaKind::LAKEHOUSE:
		result = DeltaClassicSchemaKind::LAKEHOUSE_SCHEMA;
		return true;
	default:
		return false;
	}
}

static unique_ptr<DeltaClassicSchemaEntry> MakeSchema(Catalog &catalog, const string &name, const string &path,
                                                      DeltaClassicSchemaKind kind,
                                                      optional_ptr<SchemaCatalogEntry> parent) {
	CreateSchemaInfo info;
	info.SetQualifiedName(QualifiedName({Identifier(name)}, Identifier()));
	return make_uniq<DeltaClassicSchemaEntry>(catalog, info, path, kind, parent);
}

DeltaClassicSchemaEntry &DeltaClassicSchemaEntry::AddChild(const string &name, const string &path,
                                                           DeltaClassicSchemaKind child_kind,
                                                           optional_ptr<const DeltaClassicListing> listing) {
	{
		lock_guard<mutex> guard(child_lock);
		auto existing = children.find(name);
		if (existing != children.end()) {
			// Case-insensitive collision (e.g. "Sales" and "sales" on S3): the first one listed wins
			return *existing->second;
		}
	}
	auto child = MakeSchema(catalog, name, path, child_kind, this);
	if (listing) {
		// Fill it before anyone can see it
		child->ApplyListing(*listing);
	}
	lock_guard<mutex> guard(child_lock);
	auto existing = children.find(name);
	if (existing != children.end()) {
		return *existing->second;
	}
	auto &result = *child;
	children[name] = std::move(child);
	return result;
}

optional_ptr<DeltaClassicSchemaEntry> DeltaClassicSchemaEntry::FindChild(const string &name) {
	lock_guard<mutex> guard(child_lock);
	auto entry = children.find(name);
	if (entry == children.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void DeltaClassicSchemaEntry::SetListedTables(const vector<pair<string, string>> &entries) {
	lock_guard<mutex> guard(listing_lock);
	tables.SetEntries(entries);
	listed = true;
}

void DeltaClassicSchemaEntry::ApplyListing(const DeltaClassicListing &listing) {
	vector<pair<string, string>> entries;
	if (HoldsTables()) {
		for (auto &table : listing.tables) {
			entries.emplace_back(table, DeltaClassicJoinPath(schema_path, table));
		}
	}
	// Tables directly in a lakehouse's Tables directory (a lakehouse without schemas) are left out
	tables.SetEntries(entries);
	auto child_kind = DeltaClassicSchemaKind::DIRECTORY;
	if (ChildKind(child_kind)) {
		for (auto &directory : listing.directories) {
			optional_ptr<const DeltaClassicListing> child_listing;
			if (listing.recursive) {
				// A recursive listing already saw everything below, so the nested schema isn't listed again
				auto entry = listing.children.find(directory);
				if (entry != listing.children.end()) {
					child_listing = &entry->second;
				}
			}
			AddChild(directory, DeltaClassicJoinPath(schema_path, directory), child_kind, child_listing);
		}
	}
	listed = true;
}

void DeltaClassicSchemaEntry::EnsureListed(ClientContext &context) {
	if (listed) {
		return;
	}
	lock_guard<mutex> guard(listing_lock);
	if (listed) {
		return;
	}
	if (kind == DeltaClassicSchemaKind::PLACEHOLDER) {
		tables.SetEntries({});
		listed = true;
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	ApplyListing(DeltaClassicListDirectory(fs, schema_path));
}

optional_ptr<DeltaClassicTableEntry> DeltaClassicSchemaEntry::ClaimTable(ClientContext &context,
                                                                         const CreateTableInfo &base) {
	auto &dc_catalog = catalog.Cast<DeltaClassicCatalog>();
	if (!dc_catalog.writable) {
		throw BinderException("delta_classic databases are read-only");
	}
	auto &table_name = base.GetTableName().GetIdentifierName();
	if (dc_catalog.layout == DeltaClassicLayout::SINGLE_TABLE) {
		throw BinderException("delta_classic: \"%s\" is attached as a single Delta table, so it can't hold new tables",
		                      catalog.GetName());
	}
	if (!HoldsTables()) {
		if (kind == DeltaClassicSchemaKind::LAKEHOUSE) {
			throw BinderException("delta_classic: \"%s\" is a Fabric lakehouse, so create the table in one of its "
			                      "schemas, e.g. %s.dbo.%s",
			                      GetSchemaName(), GetSchemaName(), table_name);
		}
		// A table directly in the base directory would turn the database into a single schema
		throw BinderException("delta_classic: \"%s\" has one schema per directory, so create the table in one of them",
		                      catalog.GetName());
	}
	if (base.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("delta_classic does not support CREATE OR REPLACE TABLE");
	}
	// Claims the name, so a concurrent CREATE of the same table sees it as taken
	auto entry = tables.AddTable(context, table_name, DeltaClassicJoinPath(schema_path, table_name));
	if (!entry) {
		if (base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		throw CatalogException("Table with name \"%s\" already exists in \"%s.%s\"", table_name, catalog.GetName(),
		                       GetSchemaName());
	}
	auto expected = DeltaClassicLayout::EMPTY;
	if (schema_path == dc_catalog.base_path) {
		// The first table of an empty database makes it a single schema
		dc_catalog.layout.compare_exchange_strong(expected, DeltaClassicLayout::SINGLE_SCHEMA);
	}
	return entry;
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                                BoundCreateTableInfo &info) {
	if (!transaction.HasContext()) {
		throw InternalException("delta_classic: CREATE TABLE without a client context");
	}
	auto &context = transaction.GetContext();
	auto &base = info.Base();
	auto entry = ClaimTable(context, base);
	if (!entry) {
		return nullptr;
	}
	try {
		// The Delta extension creates the table: it validates the columns, PARTITIONED BY and WITH properties
		auto &inner_catalog = entry->GetAttachedCatalog(context);
		auto inner_transaction = inner_catalog.GetCatalogTransaction(context);
		auto &inner_schema = inner_catalog.GetSchema(inner_transaction, Identifier::DefaultSchema());
		auto inner_base = base.Copy();
		inner_base->SetQualification(inner_catalog.GetName(), Identifier::DefaultSchema());
		inner_base->dependencies = LogicalDependencyList();
		BoundCreateTableInfo inner_info(inner_schema, std::move(inner_base));
		inner_schema.CreateTable(inner_transaction, inner_info);
	} catch (std::exception &) {
		tables.RemoveTable(*entry);
		throw;
	}
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(base.GetTableName()));
	return tables.GetEntry(context, lookup);
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateChildSchema(ClientContext &context, const string &child,
                                                                      OnCreateConflict on_conflict) {
	auto &dc_catalog = catalog.Cast<DeltaClassicCatalog>();
	auto child_kind = DeltaClassicSchemaKind::DIRECTORY;
	if (!ChildKind(child_kind)) {
		if (kind == DeltaClassicSchemaKind::LAKEHOUSE_SCHEMA) {
			throw BinderException("delta_classic: \"%s\" is a schema of a Fabric lakehouse, and those don't nest",
			                      GetSchemaName());
		}
		throw BinderException("delta_classic: \"%s.%s\" can't hold schemas, so create the schema at the top level",
		                      catalog.GetName(), GetSchemaName());
	}
	if (dc_catalog.layout == DeltaClassicLayout::EMPTY && schema_path == dc_catalog.base_path) {
		// The directory would be read back as a top-level schema
		throw BinderException("delta_classic: \"%s\" is empty, so create the schema at the top level: CREATE SCHEMA "
		                      "%s.%s",
		                      catalog.GetName(), catalog.GetName(), child);
	}
	EnsureListed(context);
	if (FindChild(child)) {
		if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		throw CatalogException::EntryAlreadyExists(CatalogType::SCHEMA_ENTRY, Identifier(child));
	}
	auto path = DeltaClassicJoinPath(schema_path, child);
	DeltaClassicCreateDirectory(context, path);
	DeltaClassicListing empty;
	return &AddChild(child, path, child_kind, &empty);
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                   CreateFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                                TableCatalogEntry &table) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE INDEX"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE VIEW"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                   CreateSequenceInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE SEQUENCE"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                        CreateTableFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                       CreateCopyFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                         CreatePragmaFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                    CreateCollationInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE COLLATION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE TYPE"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                                const EntryLookupInfo &lookup_info) {
	switch (lookup_info.GetCatalogType()) {
	case CatalogType::SCHEMA_ENTRY:
		// A nested schema: <this schema>.<name>
		if (transaction.HasContext()) {
			EnsureListed(transaction.GetContext());
		}
		return FindChild(lookup_info.GetEntryName()).get();
	case CatalogType::TABLE_ENTRY:
		if (!transaction.HasContext()) {
			return nullptr;
		}
		return tables.GetEntry(transaction.GetContext(), lookup_info);
	default:
		return nullptr;
	}
}

void DeltaClassicSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                   const std::function<void(CatalogEntry &)> &callback) {
	switch (type) {
	case CatalogType::SCHEMA_ENTRY: {
		EnsureListed(context);
		Scan(type, callback);
		break;
	}
	case CatalogType::TABLE_ENTRY:
		tables.Scan(context, callback);
		break;
	default:
		break;
	}
}

void DeltaClassicSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	switch (type) {
	case CatalogType::SCHEMA_ENTRY: {
		vector<reference<DeltaClassicSchemaEntry>> result;
		{
			lock_guard<mutex> guard(child_lock);
			for (auto &entry : children) {
				result.push_back(*entry.second);
			}
		}
		for (auto &schema : result) {
			callback(schema.get());
		}
		break;
	}
	case CatalogType::TABLE_ENTRY:
		tables.ScanNoContext(callback);
		break;
	default:
		break;
	}
}

void DeltaClassicSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("DROP"));
}

void DeltaClassicSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("ALTER"));
}

} // namespace duckdb
