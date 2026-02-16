#include "storage/delta_classic_table_set.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_table_entry.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

DeltaClassicTableSet::DeltaClassicTableSet(DeltaClassicSchemaEntry &schema) : schema(schema), is_loaded(false) {
}

void DeltaClassicTableSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	lock_guard<mutex> lock(entry_lock);
	if (is_loaded) {
		return;
	}

	auto &catalog = schema.ParentCatalog().Cast<DeltaClassicCatalog>();
	auto &fs = FileSystem::GetFileSystem(context);
	ClientContextFileOpener opener(context);

	fs.ListFiles(schema.schema_path, [&](const string &filename, bool is_directory) {
		if (!is_directory) {
			return;
		}
		// Skip hidden and internal directories
		if (filename.empty() || filename[0] == '.' || filename[0] == '_') {
			return;
		}
		string subdir_path = schema.schema_path + "/" + filename;
		string delta_log_path = subdir_path + "/_delta_log";

		if (fs.DirectoryExists(delta_log_path, &opener)) {
			CreateTableInfo info;
			info.table = filename;
			auto entry = make_uniq<DeltaClassicTableEntry>(catalog, schema, info, subdir_path);
			tables[filename] = std::move(entry);
		}
	}, &opener);

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

} // namespace duckdb
