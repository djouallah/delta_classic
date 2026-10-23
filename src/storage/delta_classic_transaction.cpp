#include "storage/delta_classic_transaction.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_table_entry.hpp"

namespace duckdb {

DeltaClassicTransaction::DeltaClassicTransaction(DeltaClassicCatalog &catalog, TransactionManager &manager,
                                                 ClientContext &context)
    : Transaction(manager, context) {
}

DeltaClassicTransaction::~DeltaClassicTransaction() = default;

DeltaClassicTransaction &DeltaClassicTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<DeltaClassicTransaction>();
}

optional_ptr<DeltaClassicTableEntry> DeltaClassicTransaction::GetResolvedTable(const string &key) {
	lock_guard<mutex> guard(resolved_lock);
	auto entry = resolved_tables.find(key);
	if (entry == resolved_tables.end()) {
		return nullptr;
	}
	return entry->second.get();
}

DeltaClassicTableEntry &DeltaClassicTransaction::AddResolvedTable(const string &key,
                                                                  unique_ptr<DeltaClassicTableEntry> entry) {
	lock_guard<mutex> guard(resolved_lock);
	auto &slot = resolved_tables[key];
	if (!slot) {
		slot = std::move(entry);
	}
	return *slot;
}

} // namespace duckdb
