#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class FileSystem;

//! Strips trailing separators, but never those of a URL scheme ("s3://") or a filesystem root ("/", "C:\")
string DeltaClassicTrimPath(const string &path);
//! Joins a base path and a child name with exactly one '/'
string DeltaClassicJoinPath(const string &base, const string &child);
//! The last component of a path ("s3://bucket/lake/orders/" -> "orders")
string DeltaClassicLastComponent(const string &path);

//! What one listing of a directory found
struct DeltaClassicListing {
	//! The listed directory is itself a Delta table (it contains _delta_log)
	bool is_delta_table = false;
	//! The filesystem returned a recursive listing (object stores do), so nested_tables is complete
	bool recursive = false;
	//! Direct children that are Delta tables
	set<string> tables;
	//! Direct children that are directories but not Delta tables
	set<string> directories;
	//! Recursive listings only: Delta tables one level further down, keyed by direct child
	map<string, set<string>> nested_tables;
};

//! Lists a directory and classifies its children. Handles both listing styles:
//! - flat (local, Azure): one entry per child, with is_directory set
//! - recursive (httpfs S3/GCS): every object key below the directory, relative to it, with is_directory false.
//!   DirectoryExists answers true for any S3 prefix, so it can't be used to probe for _delta_log there.
DeltaClassicListing DeltaClassicListDirectory(FileSystem &fs, const string &path);

} // namespace duckdb
