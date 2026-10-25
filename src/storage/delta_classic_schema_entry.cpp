#include "storage/delta_classic_schema_entry.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_path.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

DeltaClassicSchemaEntry::DeltaClassicSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const string &schema_path)
    : SchemaCatalogEntry(catalog, info), schema_path(schema_path), tables(*this) {
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                                BoundCreateTableInfo &info) {
	auto &dc_catalog = catalog.Cast<DeltaClassicCatalog>();
	if (!dc_catalog.writable) {
		throw BinderException("delta_classic databases are read-only");
	}
	if (!transaction.HasContext()) {
		throw InternalException("delta_classic: CREATE TABLE without a client context");
	}
	auto &context = transaction.GetContext();
	auto &base = info.Base();
	auto &table_name = base.GetTableName().GetIdentifierName();

	if (dc_catalog.layout == DeltaClassicLayout::SINGLE_TABLE) {
		throw BinderException("delta_classic: \"%s\" is attached as a single Delta table, so it can't hold new tables",
		                      catalog.GetName());
	}
	if (dc_catalog.layout == DeltaClassicLayout::MULTI_SCHEMA && schema_path == dc_catalog.base_path) {
		// A table directly in the base directory would turn the database into a single schema
		throw BinderException("delta_classic: \"%s\" has one schema per directory, so create the table in one of them",
		                      catalog.GetName());
	}
	if (base.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("delta_classic does not support CREATE OR REPLACE TABLE");
	}
	if (tables.GetListedEntry(context, table_name)) {
		if (base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		throw CatalogException("Table with name \"%s\" already exists in \"%s.%s\"", table_name, catalog.GetName(),
		                       name);
	}

	auto &entry = tables.AddTable(table_name, DeltaClassicJoinPath(schema_path, table_name));
	try {
		// The Delta extension creates the table: it validates the columns, PARTITIONED BY and WITH properties
		auto &inner_catalog = entry.GetAttachedCatalog(context);
		auto inner_transaction = inner_catalog.GetCatalogTransaction(context);
		auto &inner_schema = inner_catalog.GetSchema(inner_transaction, Identifier::DefaultSchema());
		auto inner_base = base.Copy();
		inner_base->SetQualification(inner_catalog.GetName(), Identifier::DefaultSchema());
		inner_base->dependencies = LogicalDependencyList();
		BoundCreateTableInfo inner_info(inner_schema, std::move(inner_base));
		inner_schema.CreateTable(inner_transaction, inner_info);
	} catch (std::exception &) {
		tables.RemoveTable(table_name);
		throw;
	}
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(base.GetTableName()));
	return tables.GetEntry(context, lookup);
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                   CreateFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                                TableCatalogEntry &table) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE INDEX"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE VIEW"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                   CreateSequenceInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE SEQUENCE"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                        CreateTableFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                       CreateCopyFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                         CreatePragmaFunctionInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE FUNCTION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                    CreateCollationInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE COLLATION"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("CREATE TYPE"));
}

optional_ptr<CatalogEntry> DeltaClassicSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                                const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	if (!transaction.HasContext()) {
		return nullptr;
	}
	return tables.GetEntry(transaction.GetContext(), lookup_info);
}

void DeltaClassicSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                   const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	tables.Scan(context, callback);
}

void DeltaClassicSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	tables.ScanNoContext(callback);
}

void DeltaClassicSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("DROP"));
}

void DeltaClassicSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw BinderException(catalog.Cast<DeltaClassicCatalog>().UnsupportedWriteMessage("ALTER"));
}

} // namespace duckdb
