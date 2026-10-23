#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DeltaClassicSchemaEntry;

class DeltaClassicCatalog : public Catalog {
public:
	//! delta_options are passed to every internal ATTACH ... (TYPE DELTA)
	DeltaClassicCatalog(AttachedDatabase &db, const string &base_path, unordered_map<string, Value> delta_options);
	~DeltaClassicCatalog() override;

	string base_path;
	//! Options forwarded to each internal ATTACH (pin_snapshot, timestamp, pushdown_filters, pushdown_partition_info)
	unordered_map<string, Value> delta_options;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;
	//! AT (VERSION => ...) / AT (TIMESTAMP => ...) is forwarded to the Delta extension
	bool SupportsTimeTravel() const override {
		return true;
	}

	void OnDetach(ClientContext &context) override;

	//! A fresh internal database name, unique per process, and registered for cleanup on DETACH
	string CreateInternalDbName();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
	void DiscoverSchemas(ClientContext &context);
	DeltaClassicSchemaEntry &AddSchema(const string &name, const string &schema_path);

private:
	case_insensitive_map_t<unique_ptr<DeltaClassicSchemaEntry>> schemas;
	atomic<bool> schemas_loaded;
	mutex schema_lock;

	idx_t catalog_id;
	idx_t next_internal_db = 0;
	vector<string> internal_db_names;
	mutex internal_db_lock;
};

} // namespace duckdb
