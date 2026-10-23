#include "storage/delta_classic_table_entry.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_transaction.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

DeltaClassicTableEntry::DeltaClassicTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                               const string &delta_table_path)
    : TableCatalogEntry(catalog, schema, info), delta_table_path(delta_table_path) {
}

DeltaClassicTableEntry::DeltaClassicTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                               const string &delta_table_path, TableCatalogEntry &inner_p)
    : TableCatalogEntry(catalog, schema, info), delta_table_path(delta_table_path),
      columns(inner_p.GetColumns().Copy()), inner(&inner_p) {
}

const ColumnList &DeltaClassicTableEntry::GetColumns() const {
	return columns;
}

AttachedDatabase &DeltaClassicTableEntry::EnsureAttached(ClientContext &context) {
	lock_guard<mutex> guard(attach_lock);
	auto &db_manager = DatabaseManager::Get(context);
	if (!internal_db_name.empty()) {
		auto existing = db_manager.GetDatabase(context, Identifier(internal_db_name));
		if (existing) {
			return *existing;
		}
		// The ATTACH is part of the transaction that made it: a ROLLBACK detaches it again. Attach anew.
	}

	auto &dc_catalog = catalog.Cast<DeltaClassicCatalog>();
	internal_db_name = dc_catalog.CreateInternalDbName();

	// Use the programmatic attach API (not context.Query which deadlocks during binding)
	AttachInfo info;
	info.name = Identifier(internal_db_name);
	info.path = delta_table_path;
	// The Delta extension reads its options from the AttachInfo
	info.options = dc_catalog.delta_options;

	auto attach_options = dc_catalog.delta_options;
	attach_options["type"] = Value("delta");
	// Keep the internal database out of SHOW DATABASES / duckdb_databases()
	attach_options["hidden"] = Value::BOOLEAN(true);
	AttachOptions options(attach_options, AccessMode::READ_ONLY);

	auto attached = db_manager.AttachDatabase(context, info, options);
	if (!attached) {
		throw IOException("Failed to attach Delta table \"%s\" at \"%s\"", name, delta_table_path);
	}
	return *attached;
}

TableCatalogEntry &DeltaClassicTableEntry::LookupInner(ClientContext &context,
                                                       optional_ptr<BoundAtClause> at_clause) {
	auto &attached = EnsureAttached(context);
	auto &inner_catalog = attached.GetCatalog();
	CatalogTransaction inner_transaction(inner_catalog, context);
	auto &inner_schema = inner_catalog.GetSchema(inner_transaction, Identifier::DefaultSchema());
	// A Delta catalog holds a single table, named after the catalog
	EntryLookupInfo inner_lookup(CatalogType::TABLE_ENTRY, QualifiedName(inner_catalog.GetName()), at_clause,
	                             QueryErrorContext());
	auto entry = inner_schema.LookupEntry(inner_transaction, inner_lookup);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException("Delta table \"%s\" at \"%s\" could not be read", name, delta_table_path);
	}
	return entry->Cast<TableCatalogEntry>();
}

static string ResolvedKey(const string &path, optional_ptr<BoundAtClause> at_clause) {
	if (!at_clause) {
		return path;
	}
	return path + "\n" + at_clause->Unit().GetIdentifierName() + "\n" + at_clause->GetValue().ToString();
}

DeltaClassicTableEntry &DeltaClassicTableEntry::Resolve(ClientContext &context,
                                                        optional_ptr<BoundAtClause> at_clause) {
	if (inner) {
		return *this;
	}
	auto &transaction = DeltaClassicTransaction::Get(context, catalog);
	auto key = ResolvedKey(delta_table_path, at_clause);
	auto existing = transaction.GetResolvedTable(key);
	if (existing) {
		return *existing;
	}
	auto &inner_entry = LookupInner(context, at_clause);
	CreateTableInfo info(schema, name);
	auto resolved = make_uniq<DeltaClassicTableEntry>(catalog, schema, info, delta_table_path, inner_entry);
	return transaction.AddResolvedTable(key, std::move(resolved));
}

optional_ptr<DeltaClassicTableEntry> DeltaClassicTableEntry::TryGetResolved(ClientContext &context) {
	if (inner) {
		return this;
	}
	auto &transaction = DeltaClassicTransaction::Get(context, catalog);
	return transaction.GetResolvedTable(ResolvedKey(delta_table_path, nullptr));
}

DeltaClassicTableEntry &DeltaClassicTableEntry::GetResolved(ClientContext &context) {
	return Resolve(context, nullptr);
}

unique_ptr<BaseStatistics> DeltaClassicTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	if (!inner) {
		return nullptr;
	}
	return inner->GetStatistics(context, column_id);
}

TableFunction DeltaClassicTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &resolved = GetResolved(context);
	return resolved.inner->GetScanFunction(context, bind_data);
}

TableFunction DeltaClassicTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                      const EntryLookupInfo &lookup_info) {
	auto &resolved = Resolve(context, lookup_info.GetAtClause());
	// The Delta extension only reads the AT clause from the lookup
	return resolved.inner->GetScanFunction(context, bind_data, lookup_info);
}

TableStorageInfo DeltaClassicTableEntry::GetStorageInfo(ClientContext &context) {
	if (!inner) {
		return TableStorageInfo();
	}
	return inner->GetStorageInfo(context);
}

virtual_column_map_t DeltaClassicTableEntry::GetVirtualColumns() const {
	if (!inner) {
		return TableCatalogEntry::GetVirtualColumns();
	}
	return inner->GetVirtualColumns();
}

vector<column_t> DeltaClassicTableEntry::GetRowIdColumns() const {
	if (!inner) {
		return TableCatalogEntry::GetRowIdColumns();
	}
	return inner->GetRowIdColumns();
}

} // namespace duckdb
