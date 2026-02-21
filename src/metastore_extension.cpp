#define DUCKDB_EXTENSION_MAIN

#include "metastore_extension.hpp"
#include "metastore_errors.hpp"
#include "metastore_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include <duckdb/storage/storage_extension.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

class MetastoreStorageExtension : public StorageExtension {
public:
	MetastoreStorageExtension() = default;
};

static void LoadInternal(ExtensionLoader &loader) {
	auto metastore_storage = make_shared_ptr<MetastoreStorageExtension>();
	loader.RegisterStorageExtension("metastore", metastore_storage);
	
	// Register extension options
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
