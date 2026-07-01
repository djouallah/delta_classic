#include "storage/delta_classic_table_set.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_table_entry.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

namespace {

string GetFirstPathComponent(const string &path) {
	auto slash_pos = path.find('/');
	if (slash_pos == string::npos) {
		return path;
	}
	return path.substr(0, slash_pos);
}

bool HasSecondPathComponent(const string &path, const string &expected) {
	auto first_slash = path.find('/');
	if (first_slash == string::npos) {
		return false;
	}
	auto second_slash = path.find('/', first_slash + 1);
	string second_component;
	if (second_slash == string::npos) {
		second_component = path.substr(first_slash + 1);
	} else {
		second_component = path.substr(first_slash + 1, second_slash - first_slash - 1);
	}
	return second_component == expected;
}

} // namespace

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
	case_insensitive_map_t<bool> table_names;

	fs.ListFiles(schema.schema_path, [&](const string &filename, bool is_directory) {
		if (filename.empty()) {
			return;
		}
		auto table_name = GetFirstPathComponent(filename);
		if (table_name.empty() || table_name[0] == '.' || table_name[0] == '_') {
			return;
		}
		if (HasSecondPathComponent(filename, "_delta_log")) {
			table_names[table_name] = true;
		}
	});

	for (auto &table_name : table_names) {
		string subdir_path = schema.schema_path + "/" + table_name.first;
		CreateTableInfo info;
		info.table = table_name.first;
		auto entry = make_uniq<DeltaClassicTableEntry>(catalog, schema, info, subdir_path);
		tables[table_name.first] = std::move(entry);
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
