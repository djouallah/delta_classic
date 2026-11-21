#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "storage/delta_classic_table_set.hpp"

namespace duckdb {

class DeltaClassicCatalog;
struct DeltaClassicListing;

//! What a schema's directory holds
enum class DeltaClassicSchemaKind : uint8_t {
	//! A plain directory: its Delta tables, and a nested schema per other subdirectory
	DIRECTORY,
	//! A Fabric lakehouse. Its directory is the lakehouse's Tables directory, whose subdirectories are its schemas
	LAKEHOUSE,
	//! A schema of a Fabric lakehouse (Tables/<schema>): Delta tables only, Fabric schemas don't nest
	LAKEHOUSE_SCHEMA,
	//! "main" of a database whose directory holds schemas, not tables: holds nothing
	PLACEHOLDER
};

class DeltaClassicSchemaEntry : public SchemaCatalogEntry {
public:
	DeltaClassicSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const string &schema_path,
	                        DeltaClassicSchemaKind kind, optional_ptr<SchemaCatalogEntry> parent);

	string schema_path;
	//! Changes once: "main" of an empty database becomes a placeholder when its first schema is created
	atomic<DeltaClassicSchemaKind> kind;

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

	//! Whether CREATE TABLE can put a table in this schema
	bool HoldsTables() const;
	//! The kind of the nested schemas this schema holds, or false when it holds none
	bool ChildKind(DeltaClassicSchemaKind &result) const;

	//! Lists the schema's directory once, filling its tables and nested schemas
	void EnsureListed(ClientContext &context);
	//! Sets the tables (as (name, path) pairs) of a schema whose directory is not listed, e.g. a single table
	void SetListedTables(const vector<pair<string, string>> &entries);
	//! Fills the tables and nested schemas from a listing of the schema's directory (a recursive listing fills the
	//! nested schemas too, at every depth)
	void ApplyListing(const DeltaClassicListing &listing);
	//! Claims the name of a table that CREATE TABLE (AS) is about to write, after checking that it can be created
	//! here. Returns nullptr when the table exists and IF NOT EXISTS was given.
	optional_ptr<DeltaClassicTableEntry> ClaimTable(ClientContext &context, const CreateTableInfo &base);
	//! CREATE SCHEMA <this schema>.<name>
	optional_ptr<CatalogEntry> CreateChildSchema(ClientContext &context, const string &name,
	                                             OnCreateConflict on_conflict);

public:
	DeltaClassicTableSet tables;

private:
	//! Adds a nested schema, filled from `listing` when one is given (otherwise listed on first use)
	DeltaClassicSchemaEntry &AddChild(const string &name, const string &path, DeltaClassicSchemaKind child_kind,
	                                  optional_ptr<const DeltaClassicListing> listing);
	optional_ptr<DeltaClassicSchemaEntry> FindChild(const string &name);

	mutex listing_lock;
	atomic<bool> listed;
	mutex child_lock;
	case_insensitive_map_t<unique_ptr<DeltaClassicSchemaEntry>> children;
};

} // namespace duckdb
