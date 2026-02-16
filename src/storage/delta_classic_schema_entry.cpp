#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_catalog.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

DeltaClassicSchemaEntry::DeltaClassicSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const string &schema_path)
    : SchemaCatalogEntry(catalog, info), schema_path(schema_path), tables(*this) {
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                                 BoundCreateTableInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                    CreateFunctionInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                                 TableCatalogEntry &table) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                    CreateSequenceInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                         CreateTableFunctionInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                        CreateCopyFunctionInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                          CreatePragmaFunctionInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                     CreateCollationInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto context = transaction.TryGetContext();
	if (!context) {
		return nullptr;
	}
	return tables.GetEntry(*context, lookup_info);
}

void DeltaClassicSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                    const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	tables.Scan(context, callback);
}

void DeltaClassicSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// Cannot scan without context (needed for filesystem)
}

void DeltaClassicSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

void DeltaClassicSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw BinderException("delta_classic databases are read-only");
}

} // namespace duckdb
