#include "delta_classic_extension.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_transaction_manager.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

static unique_ptr<Catalog> DeltaClassicAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                              AttachedDatabase &db, const string &name, AttachInfo &info,
                                              AttachOptions &options) {
	// Force read-only
	options.access_mode = AccessMode::READ_ONLY;

	string base_path = info.path;
	// Normalize: strip trailing slash
	while (!base_path.empty() && (base_path.back() == '/' || base_path.back() == '\\')) {
		base_path.pop_back();
	}

	// Check for PIN_SNAPSHOT in options
	bool pin_snapshot = false;
	auto it = options.options.find("pin_snapshot");
	if (it != options.options.end()) {
		pin_snapshot = true;
		options.options.erase(it);
	}

	return make_uniq<DeltaClassicCatalog>(db, base_path, options.access_mode, pin_snapshot);
}

static unique_ptr<TransactionManager> DeltaClassicCreateTransactionManager(
    optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db, Catalog &catalog) {
	auto &dc_catalog = catalog.Cast<DeltaClassicCatalog>();
	return make_uniq<DeltaClassicTransactionManager>(db, dc_catalog);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	auto extension = make_shared_ptr<StorageExtension>();
	extension->attach = DeltaClassicAttach;
	extension->create_transaction_manager = DeltaClassicCreateTransactionManager;
	StorageExtension::Register(config, "delta_classic", std::move(extension));
}

void DeltaClassicExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DeltaClassicExtension::Name() {
	return "delta_classic";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(delta_classic, loader) {
	duckdb::LoadInternal(loader);
}
}
