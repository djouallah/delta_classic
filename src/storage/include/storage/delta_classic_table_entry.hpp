#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class DeltaClassicCatalog;

class DeltaClassicTableEntry : public TableCatalogEntry {
public:
	DeltaClassicTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                       const string &delta_table_path);

	string delta_table_path;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	//! Ensures the internal delta database is attached, returns the internal table entry
	TableCatalogEntry &GetInternalTableEntry(ClientContext &context);

	//! Internal database name used for ATTACH
	string internal_db_name;
	bool is_attached;
};

} // namespace duckdb
