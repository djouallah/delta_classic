#include "storage/delta_classic_transaction.hpp"
#include "storage/delta_classic_catalog.hpp"

namespace duckdb {

DeltaClassicTransaction::DeltaClassicTransaction(DeltaClassicCatalog &catalog, TransactionManager &manager,
                                                 ClientContext &context)
    : Transaction(manager, context) {
}

DeltaClassicTransaction::~DeltaClassicTransaction() = default;

DeltaClassicTransaction &DeltaClassicTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<DeltaClassicTransaction>();
}

} // namespace duckdb
