#include "auth/metastore_secret_bridge.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static std::string GetOptionString(const case_insensitive_map_t<Value> &options, const std::string &key) {
	auto it = options.find(key);
	if (it == options.end()) {
		return "";
	}
	return StringValue::Get(it->second);
}

MetastoreProviderType InferProviderType(const std::string &provider_str) {
	auto lower = StringUtil::Lower(provider_str);
	if (lower == "hms") {
		return MetastoreProviderType::HMS;
	} else if (lower == "glue") {
		return MetastoreProviderType::Glue;
	} else if (lower == "dataproc") {
		return MetastoreProviderType::Dataproc;
	}
	return MetastoreProviderType::Unknown;
}

static void ValidateHMS(const MetastoreConnectorConfig &config) {
	if (config.endpoint.empty()) {
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig,
		    MetastoreErrorTag {"hms", "ResolveConnectorConfig", false},
		    "HMS provider requires ENDPOINT. "
		    "Example: ATTACH 'metastore' (PROVIDER 'hms', ENDPOINT 'thrift://hms-host:9083')");
	}
}

static void ValidateGlue(const MetastoreConnectorConfig &config) {
	if (!config.region.has_value() || config.region->empty()) {
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig,
		    MetastoreErrorTag {"glue", "ResolveConnectorConfig", false},
		    "Glue provider requires REGION. "
		    "Example: ATTACH 'metastore' (PROVIDER 'glue', REGION 'us-east-1')");
	}
}

static void ValidateDataproc(const MetastoreConnectorConfig &config) {
	if (config.endpoint.empty()) {
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig,
		    MetastoreErrorTag {"dataproc", "ResolveConnectorConfig", false},
		    "Dataproc provider requires ENDPOINT. "
		    "Example: ATTACH 'metastore' (PROVIDER 'dataproc', ENDPOINT 'https://dataproc.googleapis.com/...')");
	}
}

static void ResolveSecret(const case_insensitive_map_t<Value> &options, MetastoreConnectorConfig &config) {
	auto secret_name = GetOptionString(options, "SECRET");
	if (secret_name.empty()) {
		return;
	}
	// TODO(metastore): Resolve credentials via DuckDB SecretManager.
	// The DuckDB submodule is not initialized, so we cannot link against
	// SecretManager yet. When available, use:
	//   auto &secret_manager = SecretManager::Get(context);
	//   auto secret = secret_manager.GetSecretByName(transaction, secret_name);
	// Then extract key-value pairs from the secret into config.extra_params.
	config.extra_params["secret_name"] = secret_name;
}

MetastoreConnectorConfig ResolveConnectorConfig(const case_insensitive_map_t<Value> &options) {
	auto provider_str = GetOptionString(options, "PROVIDER");
	if (provider_str.empty()) {
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig,
		    MetastoreErrorTag {"unknown", "ResolveConnectorConfig", false},
		    "PROVIDER option is required. Supported providers: 'hms', 'glue', 'dataproc'. "
		    "Example: ATTACH 'metastore' (PROVIDER 'hms', ENDPOINT 'thrift://hms-host:9083')");
	}

	MetastoreConnectorConfig config;
	config.provider = InferProviderType(provider_str);

	if (config.provider == MetastoreProviderType::Unknown) {
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig,
		    MetastoreErrorTag {"unknown", "ResolveConnectorConfig", false},
		    "Unrecognized provider '" + provider_str + "'. "
		    "Supported providers: 'hms', 'glue', 'dataproc'.");
	}

	config.endpoint = GetOptionString(options, "ENDPOINT");

	auto region_str = GetOptionString(options, "REGION");
	if (!region_str.empty()) {
		config.region = region_str;
	}

	config.auth_strategy_class = GetOptionString(options, "AUTH_STRATEGY");
	if (config.auth_strategy_class.empty()) {
		config.auth_strategy_class = "StaticKeys";
	}

	ResolveSecret(options, config);

	auto provider_name = MetastoreProviderTypeToString(config.provider);
	switch (config.provider) {
	case MetastoreProviderType::HMS:
		ValidateHMS(config);
		break;
	case MetastoreProviderType::Glue:
		ValidateGlue(config);
		break;
	case MetastoreProviderType::Dataproc:
		ValidateDataproc(config);
		break;
	default:
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig,
		    MetastoreErrorTag {provider_name, "ResolveConnectorConfig", false},
		    "Provider '" + std::string(provider_name) + "' is not yet supported.");
	}

	return config;
}

} // namespace duckdb
