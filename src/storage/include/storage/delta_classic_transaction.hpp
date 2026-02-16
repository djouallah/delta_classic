#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class DeltaClassicCatalog;

class DeltaClassicTransaction : public Transaction {
public:
	DeltaClassicTransaction(DeltaClassicCatalog &catalog, TransactionManager &manager, ClientContext &context);
	~DeltaClassicTransaction() override;

	static DeltaClassicTransaction &Get(ClientContext &context, Catalog &catalog);
};

} // namespace duckdb
