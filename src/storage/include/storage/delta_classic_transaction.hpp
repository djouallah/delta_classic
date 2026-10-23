#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class DeltaClassicCatalog;
class DeltaClassicTableEntry;

class DeltaClassicTransaction : public Transaction {
public:
	DeltaClassicTransaction(DeltaClassicCatalog &catalog, TransactionManager &manager, ClientContext &context);
	~DeltaClassicTransaction() override;

	static DeltaClassicTransaction &Get(ClientContext &context, Catalog &catalog);

	//! Table entries resolved in this transaction, keyed by table path and AT clause. They live as long as the
	//! transaction, so every statement in it binds against the same snapshot and columns.
	optional_ptr<DeltaClassicTableEntry> GetResolvedTable(const string &key);
	DeltaClassicTableEntry &AddResolvedTable(const string &key, unique_ptr<DeltaClassicTableEntry> entry);

private:
	mutex resolved_lock;
	unordered_map<string, unique_ptr<DeltaClassicTableEntry>> resolved_tables;
};

} // namespace duckdb
