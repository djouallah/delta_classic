#include "storage/delta_classic_table_set.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_path.hpp"
#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_table_entry.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

DeltaClassicTableSet::DeltaClassicTableSet(DeltaClassicSchemaEntry &schema) : schema(schema), is_loaded(false) {
}

void DeltaClassicTableSet::AddEntry(const string &name, const string &path) {
	if (tables.find(name) != tables.end()) {
		// Case-insensitive collision (e.g. "Orders" and "orders" on S3): the first one listed wins
		return;
	}
	auto &catalog = schema.ParentCatalog().Cast<DeltaClassicCatalog>();
	CreateTableInfo info;
	info.table = name;
	tables[name] = make_uniq<DeltaClassicTableEntry>(catalog, schema, info, path);
}

void DeltaClassicTableSet::SetEntries(const vector<pair<string, string>> &entries) {
	lock_guard<mutex> lock(entry_lock);
	for (auto &entry : entries) {
		AddEntry(entry.first, entry.second);
	}
	is_loaded = true;
}

void DeltaClassicTableSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	lock_guard<mutex> lock(entry_lock);
	if (is_loaded) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto listing = DeltaClassicListDirectory(fs, schema.schema_path);
	for (auto &table_name : listing.tables) {
		AddEntry(table_name, DeltaClassicJoinPath(schema.schema_path, table_name));
	}

	is_loaded = true;
}

optional_ptr<CatalogEntry> DeltaClassicTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	LoadEntries(context);
	lock_guard<mutex> lock(entry_lock);
	auto &name = lookup.GetEntryName();
	auto it = tables.find(name);
	if (it == tables.end()) {
		return nullptr;
	}
	return it->second.get();
}

void DeltaClassicTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	LoadEntries(context);
	lock_guard<mutex> lock(entry_lock);
	for (auto &entry : tables) {
		callback(*entry.second);
	}
}

void DeltaClassicTableSet::ScanNoContext(const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> lock(entry_lock);
	if (!is_loaded) {
		return;
	}
	for (auto &entry : tables) {
		callback(*entry.second);
	}
}

} // namespace duckdb
