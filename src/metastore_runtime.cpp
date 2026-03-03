#include "metastore_runtime.hpp"
#include "duckdb/common/string_util.hpp"
#include "metastore_types.hpp"
#include <mutex>
#include <unordered_map>

namespace duckdb {

std::mutex &get_runtime_mutex() {
	static std::mutex mtx;
	return mtx;
}

std::unordered_map<std::string, MetastoreCatalogConfig> &get_runtime_configs() {
	static std::unordered_map<std::string, MetastoreCatalogConfig> configs;
	return configs;
}

std::unordered_map<std::string, duckdb::unique_ptr<IConnectorFactory>> &get_registry_factories() {
	static std::unordered_map<std::string, duckdb::unique_ptr<IConnectorFactory>> factories;
	return factories;
}

void RegisterMetastoreAttachConfig(const std::string &catalog_name, MetastoreCatalogConfig config) {
	std::lock_guard<std::mutex> lock(get_runtime_mutex());
	get_runtime_configs()[StringUtil::Lower(catalog_name)] = std::move(config);
}

std::optional<MetastoreCatalogConfig> LookupMetastoreAttachConfig(const std::string &catalog_name) {
	std::lock_guard<std::mutex> lock(get_runtime_mutex());
	auto it = get_runtime_configs().find(StringUtil::Lower(catalog_name));
	if (it == get_runtime_configs().end()) {
		return std::nullopt;
	}
	return it->second;
}

void ProviderRegistry::Register(const std::string &provider_id, duckdb::unique_ptr<IConnectorFactory> factory) {
	std::lock_guard<std::mutex> lock(get_runtime_mutex());
	get_registry_factories()[StringUtil::Lower(provider_id)] = std::move(factory);
}

IConnectorFactory *ProviderRegistry::GetFactory(const std::string &provider_id) {
	std::lock_guard<std::mutex> lock(get_runtime_mutex());
	auto it = get_registry_factories().find(StringUtil::Lower(provider_id));
	if (it == get_registry_factories().end()) {
		return nullptr;
	}
	return it->second.get();
}

IConnectorFactory *ProviderRegistry::ResolveProvider(const ParsedUri &uri) {
	return GetFactory(uri.scheme);
}

duckdb::unique_ptr<IMetastoreConnector> CreateConnector(const std::string &catalog_name) {
	auto config_opt = LookupMetastoreAttachConfig(catalog_name);
	if (!config_opt.has_value()) {
		throw BinderException("Catalog is not attached as metastore, or not found: " + catalog_name);
	}

	auto parsed_uri = ParsedUri::Parse(config_opt->endpoint);
	auto factory = ProviderRegistry::ResolveProvider(parsed_uri);
	if (!factory) {
		throw BinderException("No metastore provider factory found for endpoint: " + config_opt->endpoint);
	}

	return factory->CreateConnector(*config_opt);
}

} // namespace duckdb
