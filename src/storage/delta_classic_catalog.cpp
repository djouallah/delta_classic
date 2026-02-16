#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_schema_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

DeltaClassicCatalog::DeltaClassicCatalog(AttachedDatabase &db, const string &base_path, AccessMode access_mode,
                                         bool pin_snapshot)
    : Catalog(db), base_path(base_path), access_mode(access_mode), pin_snapshot(pin_snapshot), schemas_loaded(false) {
}

DeltaClassicCatalog::~DeltaClassicCatalog() = default;

void DeltaClassicCatalog::Initialize(bool load_builtin) {
}

string DeltaClassicCatalog::GetCatalogType() {
	return "delta_classic";
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

	// First pass: check if any immediate child has _delta_log (single-schema mode)
	bool has_direct_delta_tables = false;
	vector<string> child_dirs;

	fs.ListFiles(base_path, [&](const string &filename, bool is_directory) {
		if (!is_directory) {
			return;
		}
		if (filename == "_delta_log") {
			// The base_path itself is a delta table - not a directory of tables
			return;
		}
		if (filename.empty() || filename[0] == '.') {
			return;
		}
		string child_path = base_path + "/" + filename;
		string delta_log_path = child_path + "/_delta_log";
		if (fs.DirectoryExists(delta_log_path)) {
			has_direct_delta_tables = true;
		}
		child_dirs.push_back(filename);
	});

	if (has_direct_delta_tables) {
		// Single-schema mode: all delta tables are direct children
		// Schema name = last component of base_path
		string schema_name;
		auto last_slash = base_path.find_last_of('/');
		if (last_slash != string::npos) {
			schema_name = base_path.substr(last_slash + 1);
		} else {
			schema_name = base_path;
		}
		// Also handle backslashes for Windows paths
		auto last_backslash = schema_name.find_last_of('\\');
		if (last_backslash != string::npos) {
			schema_name = schema_name.substr(last_backslash + 1);
		}
		if (schema_name.empty()) {
			schema_name = DEFAULT_SCHEMA;
		}

		CreateSchemaInfo info;
		info.schema = schema_name;
		schemas[schema_name] = make_uniq<DeltaClassicSchemaEntry>(*this, info, base_path);
	} else {
		// Multi-schema mode: each child directory is a schema
		for (auto &dir_name : child_dirs) {
			if (dir_name.empty() || dir_name[0] == '_') {
				continue;
			}
			string schema_path = base_path + "/" + dir_name;
			CreateSchemaInfo info;
			info.schema = dir_name;
			schemas[dir_name] = make_uniq<DeltaClassicSchemaEntry>(*this, info, schema_path);
		}
	}

	schemas_loaded = true;
}

optional_ptr<CatalogEntry> DeltaClassicCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

void DeltaClassicCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	DiscoverSchemas(context);
	lock_guard<mutex> lock(schema_lock);
	for (auto &entry : schemas) {
		callback(*entry.second);
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

PhysicalOperator &DeltaClassicCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                          LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("delta_classic databases are read-only");
}

PhysicalOperator &DeltaClassicCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("delta_classic databases are read-only");
}

PhysicalOperator &DeltaClassicCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("delta_classic databases are read-only");
}

PhysicalOperator &DeltaClassicCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException("delta_classic databases are read-only");
}

unique_ptr<LogicalOperator> DeltaClassicCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                                  TableCatalogEntry &table,
                                                                  unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("delta_classic databases are read-only");
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
	throw NotImplementedException("delta_classic databases are read-only");
}

} // namespace duckdb
