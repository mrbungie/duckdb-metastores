#include "metastore_runtime.hpp"
#include "duckdb/common/string_util.hpp"
#include "metastore_types.hpp"
#include <mutex>
#include <unordered_map>
#include <vector>

namespace duckdb {

void RegisterHmsProvider();
void RegisterMockProvider();

struct MetastoreRuntimeState {
	DatabaseInstance *db = nullptr;
	unique_ptr<Connection> con = nullptr;
	std::vector<unique_ptr<IConnectorFactory>> factories;
	std::unordered_map<std::string, MetastoreCatalogConfig> configs;
	std::mutex mtx;
};

static MetastoreRuntimeState &GetRuntimeState() {
	static MetastoreRuntimeState state;
	return state;
}

void RegisterMetastoreAttachConfig(const std::string &catalog_name, MetastoreCatalogConfig config) {
	auto &state = GetRuntimeState();
	std::lock_guard<std::mutex> lock(state.mtx);
	state.configs[StringUtil::Lower(catalog_name)] = std::move(config);
}

std::optional<MetastoreCatalogConfig> LookupMetastoreAttachConfig(const std::string &catalog_name) {
	auto &state = GetRuntimeState();
	std::lock_guard<std::mutex> lock(state.mtx);
	auto it = state.configs.find(StringUtil::Lower(catalog_name));
	if (it == state.configs.end()) {
		return std::nullopt;
	}
	return it->second;
}

void ProviderRegistry::Register(duckdb::unique_ptr<IConnectorFactory> factory) {
	auto &state = GetRuntimeState();
	std::lock_guard<std::mutex> lock(state.mtx);
	state.factories.push_back(std::move(factory));
}

IConnectorFactory *ProviderRegistry::ResolveProvider(const ParsedUri &uri) {
	auto &state = GetRuntimeState();
	std::lock_guard<std::mutex> lock(state.mtx);
	if (state.factories.empty()) {
		// Lazy initialize if not already done
		RegisterHmsProvider();
		RegisterMockProvider();
	}
	for (auto &factory : state.factories) {
		if (factory->CanHandle(uri)) {
			return factory.get();
		}
	}
	return nullptr;
}

void ProviderRegistry::Initialize() {
	RegisterHmsProvider();
	RegisterMockProvider();
}

void MetastoreRuntime::SetDatabase(DatabaseInstance &db) {
	auto &state = GetRuntimeState();
	std::lock_guard<std::mutex> lock(state.mtx);
	state.db = &db;
	state.con = make_uniq<Connection>(db);
}

Connection &MetastoreRuntime::GetConnection() {
	auto &state = GetRuntimeState();
	std::lock_guard<std::mutex> lock(state.mtx);
	if (!state.con) {
		throw InternalException("MetastoreRuntime::GetConnection called before SetDatabase");
	}
	return *state.con;
}

duckdb::unique_ptr<IMetastoreConnector> CreateConnector(const std::string &catalog_name) {
	auto config_opt = LookupMetastoreAttachConfig(catalog_name);
	if (!config_opt.has_value()) {
		throw BinderException("Catalog is not attached as metastore, or not found: " + catalog_name);
	}

	auto parsed_uri = ParsedUri::Parse(config_opt->endpoint);
	auto factory = ProviderRegistry::ResolveProvider(parsed_uri);
	if (!factory) {
		throw BinderException("Invalid Error: Could not infer metastore provider from endpoint. Use thrift://, "
		                      "thrift+http(s)://, or http(s):// for HMS, arn:aws:glue: for Glue, or "
		                      "https://...dataproc... for Dataproc.");
	}

	return factory->CreateConnector(*config_opt);
}

} // namespace duckdb
