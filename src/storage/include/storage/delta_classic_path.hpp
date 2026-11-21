#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class ClientContext;
class FileSystem;

//! Strips trailing separators, but never those of a URL scheme ("s3://") or a filesystem root ("/", "C:\")
string DeltaClassicTrimPath(const string &path);
//! Joins a base path and a child name with exactly one '/'
string DeltaClassicJoinPath(const string &base, const string &child);
//! The last component of a path ("s3://bucket/lake/orders/" -> "orders")
string DeltaClassicLastComponent(const string &path);

//! The directory of a Fabric lakehouse's managed tables
static constexpr const char *DELTA_CLASSIC_LAKEHOUSE_TABLES = "Tables";
//! The schema name of a Fabric lakehouse item directory ("sales.Lakehouse" -> "sales"), or "" for any other name
string DeltaClassicLakehouseName(const string &item_directory);
//! Whether the path is the root of a OneLake workspace ("abfss://<workspace>@onelake.dfs.fabric.microsoft.com/")
bool DeltaClassicIsOneLakeWorkspace(const string &path);
//! Whether a listing of a lakehouse's Tables directory is that of a lakehouse without schemas (tables only)
struct DeltaClassicListing;
bool DeltaClassicIsLakehouseWithoutSchemas(const DeltaClassicListing &tables_listing);
//! The error for a lakehouse without schemas, which delta_classic does not read
string DeltaClassicLakehouseWithoutSchemasError(const string &lakehouse, const string &tables_path);

//! What one listing of a directory found
struct DeltaClassicListing {
	//! The listed directory is itself a Delta table (it contains _delta_log)
	bool is_delta_table = false;
	//! The filesystem returned a recursive listing (object stores do), so `children` is complete at every depth
	bool recursive = false;
	//! Direct children that are Delta tables
	set<string> tables;
	//! Direct children that are directories but not Delta tables
	set<string> directories;
	//! Recursive listings only: the listing of each directory in `directories`
	map<string, DeltaClassicListing> children;
};

//! Lists a directory and classifies its children. Handles both listing styles:
//! - flat (local, Azure): one entry per child, with is_directory set
//! - recursive (httpfs S3/GCS): every object key below the directory, relative to it, with is_directory false.
//!   DirectoryExists answers true for any S3 prefix, so it can't be used to probe for _delta_log there.
DeltaClassicListing DeltaClassicListDirectory(FileSystem &fs, const string &path);

//! Creates a schema's directory. Object stores (S3, GCS) have no directories: there a new schema lasts until it gets
//! its first table, or until DETACH.
void DeltaClassicCreateDirectory(ClientContext &context, const string &path);

} // namespace duckdb
