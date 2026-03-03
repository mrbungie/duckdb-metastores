#define DUCKDB_EXTENSION_MAIN

#include "metastore_functions.hpp"
#include "metastore_extension.hpp"
#include "metastore_duckdb.hpp"
#include "metastore_runtime.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	MetastoreRuntime::SetDatabase(instance);

	auto &config = DBConfig::GetConfig(instance);

	// Register providers
	ProviderRegistry::Initialize();

	config.storage_extensions["metastore"] = CreateMetastoreStorageExtension();
	config.replacement_scans.emplace_back(MetastoreReplacementScan);
	config.AddExtensionOption("metastore_debug", "Enable diagnostic mode for metastore operations",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("metastore_max_partitions", "Maximum number of partitions to list for a table",
	                          LogicalType::BIGINT, Value::BIGINT(100000));

	// Register functions
	// We need a context or something to register functions?
	// ExtensionLoader::RegisterFunction just takes the name and some pointers.
	// Iceberg Table Functions
	for (auto &fun : MetastoreFunctions::GetTableFunctions(loader)) {
		loader.RegisterFunction(std::move(fun));
	}

	// Iceberg Scalar Functions
	for (auto &fun : MetastoreFunctions::GetScalarFunctions()) {
		loader.RegisterFunction(fun);
	}
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
