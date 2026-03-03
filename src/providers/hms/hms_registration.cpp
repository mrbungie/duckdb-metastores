#include "metastore_runtime.hpp"
#include "providers/hms/hms_connector.hpp"
#include "providers/hms/hms_config.hpp"

namespace duckdb {

class HmsConnectorFactory : public IConnectorFactory {
public:
	bool CanHandle(const ParsedUri &uri) const override {
		return uri.scheme == "thrift" || uri.scheme == "thrift+ssl" || uri.scheme == "thrift+http" ||
		       uri.scheme == "thrift+https";
	}

	MetastoreCatalogConfig NormalizeConfig(const std::string &catalog_name, const ParsedUri &uri,
	                                       const case_insensitive_map_t<Value> &options) override {
		MetastoreCatalogConfig config;
		config.catalog_name = catalog_name;
		config.options = options;
		config.provider = MetastoreProviderType::HMS;
		// Reconstruct the full endpoint from ParsedUri if it's missing the scheme
		if (uri.scheme.empty()) {
			config.endpoint = "thrift://" + uri.authority + uri.path;
		} else {
			config.endpoint = uri.scheme + "://" + uri.authority + uri.path;
		}

		for (auto &entry : options) {
			if (entry.first == "REGION") {
				config.region = entry.second.ToString();
			} else if (entry.first == "AUTH_STRATEGY") {
				config.auth_strategy_class = entry.second.ToString();
			} else {
				config.extra_params[entry.first] = entry.second.ToString();
			}
		}
		return config;
	}

	duckdb::unique_ptr<IMetastoreConnector> CreateConnector(const MetastoreCatalogConfig &config) override {
		return duckdb::make_uniq<HmsConnector>(ParseHmsEndpoint(config.endpoint));
	}
};

void RegisterHmsProvider() {
	ProviderRegistry::Register(duckdb::make_uniq<HmsConnectorFactory>());
}

} // namespace duckdb
