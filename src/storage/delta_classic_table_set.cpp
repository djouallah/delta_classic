#include "storage/delta_classic_table_set.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_path.hpp"
#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_table_entry.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
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
	auto &catalog = schema.ParentCatalog();
	CreateTableInfo info(schema, Identifier(name));
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

optional_ptr<DeltaClassicTableEntry> DeltaClassicTableSet::FindEntry(const string &name) {
	lock_guard<mutex> lock(entry_lock);
	auto it = tables.find(name);
	if (it == tables.end()) {
		return nullptr;
	}
	return it->second.get();
}

optional_ptr<DeltaClassicTableEntry> DeltaClassicTableSet::GetListedEntry(ClientContext &context, const string &name) {
	LoadEntries(context);
	return FindEntry(name);
}

DeltaClassicTableEntry &DeltaClassicTableSet::AddTable(const string &name, const string &path) {
	lock_guard<mutex> lock(entry_lock);
	AddEntry(name, path);
	return *tables[name];
}

void DeltaClassicTableSet::RemoveTable(const string &name) {
	lock_guard<mutex> lock(entry_lock);
	tables.erase(name);
}

optional_ptr<CatalogEntry> DeltaClassicTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	LoadEntries(context);
	auto entry = FindEntry(lookup.GetEntryName());
	if (!entry) {
		return nullptr;
	}
	return entry->Resolve(context, lookup.GetAtClause());
}

void DeltaClassicTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	LoadEntries(context);
	vector<reference<DeltaClassicTableEntry>> entries;
	{
		lock_guard<mutex> lock(entry_lock);
		for (auto &entry : tables) {
			entries.push_back(*entry.second);
		}
	}
	for (auto &entry : entries) {
		// Prefer the entry this transaction already resolved, which has columns. Resolving every table here would
		// read every Delta log just to list tables.
		auto resolved = entry.get().TryGetResolved(context);
		callback(resolved ? *resolved : entry.get());
	}
}

void DeltaClassicTableSet::ScanNoContext(const std::function<void(CatalogEntry &)> &callback) {
	if (!is_loaded) {
		return;
	}
	lock_guard<mutex> lock(entry_lock);
	for (auto &entry : tables) {
		callback(*entry.second);
	}
}

} // namespace duckdb
