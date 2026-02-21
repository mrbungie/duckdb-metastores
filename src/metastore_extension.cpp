#define DUCKDB_EXTENSION_MAIN

#include "metastore_extension.hpp"
#include "metastore_errors.hpp"
#include "metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "auth/metastore_secret_bridge.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include <duckdb/storage/storage_extension.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

class MetastoreStorageExtension : public StorageExtension {
public:
	MetastoreStorageExtension() {
		attach = MetastoreAttach;
		create_transaction_manager = MetastoreCreateTransactionManager;
	}

	static unique_ptr<Catalog> MetastoreAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                           AttachedDatabase &db, const string &name, AttachInfo &info,
	                                           AttachOptions &attach_options) {
		case_insensitive_map_t<Value> attach_kv;
		for (auto &entry : info.options) {
			attach_kv[entry.first] = entry.second;
		}
		auto connector_config = ResolveConnectorConfig(attach_kv);
		if (connector_config.provider != MetastoreProviderType::HMS) {
			throw InvalidInputException("Only HMS provider is supported in this build");
		}
		RegisterMetastoreAttachConfig(name, std::move(connector_config));
		info.path = ":memory:";
		auto catalog = make_uniq<DuckCatalog>(db);
		catalog->Initialize(false);
		return std::move(catalog);
	}

	static unique_ptr<TransactionManager>
	MetastoreCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
	                                  Catalog &catalog) {
		return make_uniq<DuckTransactionManager>(db);
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	auto metastore_storage = make_shared_ptr<MetastoreStorageExtension>();
	loader.RegisterStorageExtension("metastore", metastore_storage);

	loader.RegisterExtensionOption("metastore_debug", "Enable diagnostic mode for metastore operations",
	                                LogicalType::BOOLEAN, Value::BOOLEAN(false));

	RegisterMetastoreFunctions(loader);
}

void MetastoreExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string MetastoreExtension::Name() {
	return "metastore";
}

std::string MetastoreExtension::Version() const {
#ifdef EXT_VERSION_METASTORE
	return EXT_VERSION_METASTORE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(metastore, loader) {
	duckdb::LoadInternal(loader);
}
}
