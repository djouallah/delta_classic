#include "storage/delta_classic_table_entry.hpp"
#include "storage/delta_classic_catalog.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
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

TableCatalogEntry &DeltaClassicTableEntry::GetInternalTableEntry(ClientContext &context) {
	if (!is_attached) {
		auto &dc_catalog = catalog.Cast<DeltaClassicCatalog>();

		// Build the ATTACH statement using internal SQL execution
		string sql = "ATTACH '" + delta_table_path + "' AS \"" + internal_db_name + "\" (TYPE DELTA";
		if (dc_catalog.pin_snapshot) {
			sql += ", PIN_SNAPSHOT";
		}
		sql += ");";

		auto result = context.Query(sql, false);
		if (result->HasError()) {
			throw BinderException("Failed to attach delta table '%s': %s", delta_table_path,
			                      result->GetError());
		}
		is_attached = true;
	}

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
