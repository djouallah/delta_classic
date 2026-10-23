#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class AttachedDatabase;
class BoundAtClause;
class DeltaClassicCatalog;

//! A Delta table inside a delta_classic catalog. There are two kinds of entries:
//! - listed: one per table found in storage. Owns the internal ATTACH ... (TYPE DELTA) and has no columns.
//! - resolved: created per transaction (and AT clause) by Resolve(). Copies the columns of the internal Delta
//!   table entry for that snapshot and forwards scans, statistics and virtual columns to it.
class DeltaClassicTableEntry : public TableCatalogEntry {
public:
	//! A listed entry
	DeltaClassicTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                       const string &delta_table_path);
	//! A resolved entry
	DeltaClassicTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                       const string &delta_table_path, TableCatalogEntry &inner);

	string delta_table_path;

public:
	const ColumnList &GetColumns() const override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

	//! Listed entries only: the resolved entry for this transaction and AT clause, attaching the Delta table on
	//! first use
	DeltaClassicTableEntry &Resolve(ClientContext &context, optional_ptr<BoundAtClause> at_clause);
	//! Listed entries only: the resolved entry of the current transaction without an AT clause, if any
	optional_ptr<DeltaClassicTableEntry> TryGetResolved(ClientContext &context);

private:
	//! Attaches the Delta table if it isn't (or no longer is, e.g. after a ROLLBACK) attached
	AttachedDatabase &EnsureAttached(ClientContext &context);
	//! Looks up the table in the internal Delta catalog, passing the AT clause through
	TableCatalogEntry &LookupInner(ClientContext &context, optional_ptr<BoundAtClause> at_clause);
	//! The entry that scans are forwarded to
	DeltaClassicTableEntry &GetResolved(ClientContext &context);

	ColumnList columns;
	//! Resolved entries: the internal Delta table entry
	optional_ptr<TableCatalogEntry> inner;
	//! Listed entries: guards the internal ATTACH
	mutex attach_lock;
	//! Listed entries: name of the internal Delta database (empty until first attached)
	string internal_db_name;
};

} // namespace duckdb
