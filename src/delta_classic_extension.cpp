#include "delta_classic_extension.hpp"
#include "storage/delta_classic_catalog.hpp"
#include "storage/delta_classic_path.hpp"
#include "storage/delta_classic_transaction_manager.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

//! Parses the delta_classic ATTACH options into the options passed to each internal ATTACH ... (TYPE DELTA)
static unordered_map<string, Value> ParseDeltaOptions(ClientContext &context, AttachOptions &options) {
	unordered_map<string, Value> delta_options;
	for (auto &entry : options.options) {
		auto key = StringUtil::Lower(entry.first);
		if (key == "pin_snapshot") {
			if (BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN))) {
				delta_options["pin_snapshot"] = Value::BOOLEAN(true);
			}
		} else if (key == "timestamp") {
			// Every table is read as of this point in time, in this session's time zone
			delta_options["timestamp"] = entry.second.CastAs(context, LogicalType::TIMESTAMP_TZ);
		} else if (key == "pushdown_filters" || key == "pushdown_partition_info") {
			delta_options[key] = entry.second;
		} else if (key == "version") {
			throw BinderException("delta_classic does not support VERSION: Delta versions are per table. Use "
			                      "TIMESTAMP to read every table as of a point in time, or AT (VERSION => ...) "
			                      "on a single table");
		} else {
			throw BinderException("Unrecognized option for delta_classic attach \"%s\"", entry.first);
		}
	}
	options.options.clear();
	return delta_options;
}

static unique_ptr<Catalog> DeltaClassicAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                              AttachedDatabase &db, const string &name, AttachInfo &info,
                                              AttachOptions &options) {
	// Read-only unless asked for READ_ONLY false / READ_WRITE
	bool writable = options.access_mode == AccessMode::READ_WRITE;
	auto base_path = DeltaClassicTrimPath(info.path);
	auto delta_options = ParseDeltaOptions(context, options);
	if (writable) {
		if (delta_options.find("timestamp") != delta_options.end()) {
			throw BinderException("delta_classic: TIMESTAMP reads every table as of a point in time, so it can't be "
			                      "combined with READ_ONLY false");
		}
		if (delta_options.find("pin_snapshot") != delta_options.end()) {
			throw BinderException("delta_classic: PIN_SNAPSHOT can't be combined with READ_ONLY false");
		}
	} else {
		db.SetReadOnlyDatabase();
	}
	return make_uniq<DeltaClassicCatalog>(db, base_path, std::move(delta_options), writable);
}

static unique_ptr<TransactionManager>
DeltaClassicCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                     Catalog &catalog) {
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
