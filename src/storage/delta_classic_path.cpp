#include "storage/delta_classic_path.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

static bool IsSeparator(char c) {
	return c == '/' || c == '\\';
}

static bool IsHidden(const string &name) {
	return name.empty() || name[0] == '.' || name[0] == '_';
}

string DeltaClassicTrimPath(const string &path) {
	auto scheme = path.find("://");
	idx_t min_length = scheme == string::npos ? 1 : scheme + 3;
	if (path.size() >= 3 && path[1] == ':' && IsSeparator(path[2])) {
		// Windows drive root
		min_length = MaxValue<idx_t>(min_length, 3);
	}
	auto result = path;
	while (result.size() > min_length && IsSeparator(result.back())) {
		result.pop_back();
	}
	return result;
}

string DeltaClassicJoinPath(const string &base, const string &child) {
	if (base.empty()) {
		return child;
	}
	if (IsSeparator(base.back())) {
		return base + child;
	}
	return base + "/" + child;
}

string DeltaClassicLastComponent(const string &path) {
	auto trimmed = DeltaClassicTrimPath(path);
	auto pos = trimmed.find_last_of("/\\");
	if (pos == string::npos) {
		return trimmed;
	}
	return trimmed.substr(pos + 1);
}

//! Splits a listed entry into path components relative to the listed directory
static vector<string> SplitEntry(const string &directory, const string &entry) {
	string relative = entry;
	// Tolerate filesystems that return full paths instead of names relative to the directory
	auto prefix = DeltaClassicJoinPath(DeltaClassicTrimPath(directory), "");
	if (relative.size() > prefix.size() && relative.compare(0, prefix.size(), prefix) == 0) {
		relative = relative.substr(prefix.size());
	}
	vector<string> result;
	string current;
	for (auto c : relative) {
		if (IsSeparator(c)) {
			if (!current.empty()) {
				result.push_back(std::move(current));
				current.clear();
			}
		} else {
			current += c;
		}
	}
	if (!current.empty()) {
		result.push_back(std::move(current));
	}
	return result;
}

DeltaClassicListing DeltaClassicListDirectory(FileSystem &fs, const string &path) {
	DeltaClassicListing result;
	fs.ListFiles(path, [&](const string &entry, bool is_directory) {
		auto parts = SplitEntry(path, entry);
		if (parts.empty()) {
			return;
		}
		auto &child = parts[0];
		if (child == "_delta_log") {
			result.is_delta_table = true;
			return;
		}
		if (IsHidden(child)) {
			return;
		}
		if (parts.size() > 1) {
			// Recursive listing: an object key somewhere below the child
			result.recursive = true;
			if (parts[1] == "_delta_log") {
				result.tables.insert(child);
				return;
			}
			result.directories.insert(child);
			if (parts.size() > 2 && parts[2] == "_delta_log" && !IsHidden(parts[1])) {
				result.nested_tables[child].insert(parts[1]);
			}
			return;
		}
		if (!is_directory) {
			// A plain file directly inside the directory
			return;
		}
		if (fs.DirectoryExists(DeltaClassicJoinPath(DeltaClassicJoinPath(path, child), "_delta_log"))) {
			result.tables.insert(child);
		} else {
			result.directories.insert(child);
		}
	});
	// In a recursive listing a table's data files also make it look like a plain directory
	for (auto &table : result.tables) {
		result.directories.erase(table);
		result.nested_tables.erase(table);
	}
	return result;
}

} // namespace duckdb
