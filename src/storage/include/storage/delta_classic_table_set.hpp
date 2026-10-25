#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/vector.hpp"
#include "storage/delta_classic_table_entry.hpp"

namespace duckdb {

class DeltaClassicCatalog;
class DeltaClassicSchemaEntry;
struct EntryLookupInfo;

class DeltaClassicTableSet {
public:
	explicit DeltaClassicTableSet(DeltaClassicSchemaEntry &schema);

	//! Fills the set from a listing that already found its tables, as (name, path) pairs, so it is not listed again
	void SetEntries(const vector<pair<string, string>> &entries);
	//! The entry for this transaction (and AT clause), or nullptr when no such table exists
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	void ScanNoContext(const std::function<void(CatalogEntry &)> &callback);
	//! The listed entry of a table, or nullptr when no such table exists
	optional_ptr<DeltaClassicTableEntry> GetListedEntry(ClientContext &context, const string &name);
	//! Adds a table that CREATE TABLE is about to write
	DeltaClassicTableEntry &AddTable(const string &name, const string &path);
	//! Removes a table added by AddTable whose creation failed
	void RemoveTable(const string &name);

private:
	void LoadEntries(ClientContext &context);
	void AddEntry(const string &name, const string &path);
	optional_ptr<DeltaClassicTableEntry> FindEntry(const string &name);

	DeltaClassicSchemaEntry &schema;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<DeltaClassicTableEntry>> tables;
	atomic<bool> is_loaded;
};

} // namespace duckdb
