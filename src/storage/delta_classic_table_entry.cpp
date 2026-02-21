#include "storage/delta_classic_table_entry.hpp"
#include "storage/delta_classic_catalog.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

DeltaClassicTableEntry::DeltaClassicTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                               const string &delta_table_path)
    : TableCatalogEntry(catalog, schema, info), delta_table_path(delta_table_path), is_attached(false) {
	// Generate a unique internal database name to avoid collisions
	internal_db_name = "__dc_" + catalog.GetName() + "_" + schema.name + "_" + info.table;
}

unique_ptr<BaseStatistics> DeltaClassicTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void DeltaClassicTableEntry::EnsureAttached(ClientContext &context) {
	if (is_attached) {
		return;
	}

	auto &db_manager = DatabaseManager::Get(context);

	// Check if already attached (e.g. from a previous call)
	if (db_manager.GetDatabase(context, internal_db_name)) {
		is_attached = true;
		return;
	}

	// Use the programmatic attach API (not context.Query which deadlocks during binding)
	AttachInfo info;
	info.name = internal_db_name;
	info.path = delta_table_path;
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

	unordered_map<string, Value> opts;
	opts["type"] = Value("delta");
	auto &dc_catalog = catalog.Cast<DeltaClassicCatalog>();
	if (dc_catalog.pin_snapshot) {
		opts["pin_snapshot"] = Value::BOOLEAN(true);
	}

	auto &config = DBConfig::GetConfig(context);
	AttachOptions options(opts, config.options.access_mode);

	db_manager.AttachDatabase(context, info, options);

	dc_catalog.RegisterInternalDb(internal_db_name);
	is_attached = true;
}

TableCatalogEntry &DeltaClassicTableEntry::GetInternalTableEntry(ClientContext &context) {
	EnsureAttached(context);

	// Look up the table in the internally attached delta database
	auto &db_manager = DatabaseManager::Get(context);
	auto db_entry = db_manager.GetDatabase(context, internal_db_name);
	if (!db_entry) {
		throw InternalException("Internal delta database '%s' not found after attach", internal_db_name);
	}

	auto &internal_catalog = db_entry->GetCatalog();

	// Delta attach puts the table in the default schema - scan to find it
	auto &internal_schema = internal_catalog.GetSchema(context, DEFAULT_SCHEMA);
	optional_ptr<CatalogEntry> table_entry;
	internal_schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (!table_entry) {
			table_entry = &entry;
		}
	});

	if (!table_entry) {
		throw InternalException("No table found in internally attached delta database '%s'", internal_db_name);
	}

	return table_entry->Cast<TableCatalogEntry>();
}

TableFunction DeltaClassicTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &internal_table = GetInternalTableEntry(context);
	auto result = internal_table.GetScanFunction(context, bind_data);

	// Update columns on this table entry so DESCRIBE works
	auto &internal_columns = internal_table.GetColumns();
	for (auto &col : internal_columns.Logical()) {
		if (!ColumnExists(col.Name())) {
			ColumnDefinition new_col(col.Name(), col.Type());
			columns.AddColumn(std::move(new_col));
		}
	}

	return result;
}

TableStorageInfo DeltaClassicTableEntry::GetStorageInfo(ClientContext &context) {
	return TableStorageInfo();
}

} // namespace duckdb
