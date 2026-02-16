#include "storage/delta_classic_transaction_manager.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_transaction.hpp"

namespace duckdb {

DeltaClassicTransactionManager::DeltaClassicTransactionManager(AttachedDatabase &db, DeltaClassicCatalog &catalog)
    : TransactionManager(db), catalog(catalog) {
}

Transaction &DeltaClassicTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<DeltaClassicTransaction>(catalog, *this, context);
	auto &result = *transaction;
	lock_guard<mutex> lock(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData DeltaClassicTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> lock(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void DeltaClassicTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> lock(transaction_lock);
	transactions.erase(transaction);
}

void DeltaClassicTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// No-op for read-only catalog
}

} // namespace duckdb
