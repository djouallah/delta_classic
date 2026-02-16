#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

class DeltaClassicCatalog;
class DeltaClassicSchemaEntry;
class DeltaClassicTableEntry;
struct EntryLookupInfo;

class DeltaClassicTableSet {
public:
	explicit DeltaClassicTableSet(DeltaClassicSchemaEntry &schema);

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

private:
	void LoadEntries(ClientContext &context);

	DeltaClassicSchemaEntry &schema;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<DeltaClassicTableEntry>> tables;
	bool is_loaded;
};

} // namespace duckdb
