#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class DeltaClassicCatalog;
#include "storage/delta_classic_transaction.hpp"

class DeltaClassicTransactionManager : public TransactionManager {
public:
	DeltaClassicTransactionManager(AttachedDatabase &db, DeltaClassicCatalog &catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	DeltaClassicCatalog &catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<DeltaClassicTransaction>> transactions;
};

} // namespace duckdb
