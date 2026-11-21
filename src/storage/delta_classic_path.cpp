#include "storage/delta_classic_path.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
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

string DeltaClassicLakehouseName(const string &item_directory) {
	const string suffix = ".Lakehouse";
	if (item_directory.size() <= suffix.size()) {
		return string();
	}
	auto stem_size = item_directory.size() - suffix.size();
	if (item_directory.compare(stem_size, suffix.size(), suffix) != 0) {
		return string();
	}
	return item_directory.substr(0, stem_size);
}

bool DeltaClassicIsOneLakeWorkspace(const string &path) {
	auto scheme_end = path.find("://");
	if (scheme_end == string::npos) {
		return false;
	}
	auto rest = path.substr(scheme_end + 3);
	while (!rest.empty() && IsSeparator(rest.back())) {
		rest.pop_back();
	}
	auto slash = rest.find('/');
	string host;
	string remainder;
	if (rest.find('@') != string::npos && (slash == string::npos || rest.find('@') < slash)) {
		// <workspace>@onelake.dfs.fabric.microsoft.com[/<path>]
		auto at = rest.find('@');
		host = rest.substr(at + 1, slash == string::npos ? string::npos : slash - at - 1);
		remainder = slash == string::npos ? string() : rest.substr(slash + 1);
	} else {
		// onelake.dfs.fabric.microsoft.com/<workspace>[/<path>]
		host = rest.substr(0, slash);
		auto container = slash == string::npos ? string() : rest.substr(slash + 1);
		if (container.empty() || container.find('/') != string::npos) {
			return false;
		}
	}
	if (host.find("onelake.") == string::npos || host.find(".fabric.microsoft.com") == string::npos) {
		return false;
	}
	return remainder.empty();
}

bool DeltaClassicIsLakehouseWithoutSchemas(const DeltaClassicListing &tables_listing) {
	return !tables_listing.tables.empty() && tables_listing.directories.empty();
}

string DeltaClassicLakehouseWithoutSchemasError(const string &lakehouse, const string &tables_path) {
	return "delta_classic: lakehouse \"" + lakehouse +
	       "\" has no schemas (its tables are directly in Tables), which delta_classic does not read. Attach '" +
	       tables_path + "' instead: its tables are then <db>.main.<table>";
}

//! Records one object key of a recursive listing, given as path components relative to the listed directory
static void AddRecursiveEntry(DeltaClassicListing &root, const vector<string> &parts) {
	idx_t delta_log = parts.size();
	for (idx_t i = 0; i < parts.size(); i++) {
		if (parts[i] == "_delta_log") {
			delta_log = i;
			break;
		}
	}
	if (delta_log == 0) {
		root.is_delta_table = true;
		return;
	}
	// The directories this key passes through: up to the table, or up to (not including) the file itself
	auto directory_count = delta_log < parts.size() ? delta_log : parts.size() - 1;
	auto node = &root;
	for (idx_t i = 0; i < directory_count; i++) {
		auto &child = parts[i];
		if (IsHidden(child) || node->tables.find(child) != node->tables.end()) {
			// Hidden, or below a table already found (its partition directories and data files)
			return;
		}
		if (i + 1 == delta_log) {
			node->tables.insert(child);
			return;
		}
		node->directories.insert(child);
		node = &node->children[child];
		node->recursive = true;
	}
}

//! A table's data files are listed before (or after) its _delta_log, so they also make it look like a directory
static void DropTableDirectories(DeltaClassicListing &listing) {
	for (auto &table : listing.tables) {
		listing.directories.erase(table);
		listing.children.erase(table);
	}
	for (auto &child : listing.children) {
		DropTableDirectories(child.second);
	}
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
			AddRecursiveEntry(result, parts);
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
	DropTableDirectories(result);
	return result;
}

void DeltaClassicCreateDirectory(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	try {
		// Its parents too: the attached directory may not exist yet
		fs.CreateDirectoriesRecursive(path);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() != ExceptionType::NOT_IMPLEMENTED) {
			throw;
		}
	}
}

} // namespace duckdb
