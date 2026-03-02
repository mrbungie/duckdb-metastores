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

static std::optional<std::string> ExtractGlueRegionFromArn(const std::string &endpoint) {
	// ARN format: arn:partition:service:region:account-id:resource
	if (endpoint.empty()) {
		return std::optional<std::string>();
	}
	auto lower = StringUtil::Lower(endpoint);
	if (lower.find("arn:aws:glue:") != 0) {
		return std::optional<std::string>();
	}
	auto parts = StringUtil::Split(endpoint, ':');
	if (parts.size() < 6) {
		return std::optional<std::string>();
	}
	if (parts[3].empty()) {
		return std::optional<std::string>();
	}
	return parts[3];
}

static void ValidateHMS(const MetastoreConnectorConfig &config) {
	if (config.endpoint.empty()) {
		throw_metastore_error(MetastoreErrorCode::InvalidConfig,
		                      MetastoreErrorTag {"hms", "ResolveConnectorConfig", false},
		                      "HMS requires endpoint in ATTACH path. "
		                      "Example: ATTACH 'thrift://hms-host:9083' AS hms (TYPE metastore)");
	}
}

static void ValidateGlue(const MetastoreConnectorConfig &config) {
	if (!config.region.has_value() || config.region->empty()) {
		throw_metastore_error(MetastoreErrorCode::InvalidConfig,
		                      MetastoreErrorTag {"glue", "ResolveConnectorConfig", false},
		                      "Glue requires REGION parameter. "
		                      "Example: ATTACH 'arn:aws:glue:us-east-1:123456789012:catalog' AS glue (TYPE metastore, "
		                      "REGION 'us-east-1')");
	}
}

static void ValidateDataproc(const MetastoreConnectorConfig &config) {
	if (config.endpoint.empty()) {
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig, MetastoreErrorTag {"dataproc", "ResolveConnectorConfig", false},
		    "Dataproc requires endpoint in ATTACH path. "
		    "Example: ATTACH 'https://dataproc.googleapis.com/v1/projects/...' AS dp (TYPE metastore)");
	}
}

static MetastoreProviderType InferProviderFromUrl(const std::string &endpoint) {
	if (endpoint.empty()) {
		return MetastoreProviderType::Unknown;
	}
	auto lower = StringUtil::Lower(endpoint);
	if (lower.find("thrift://") == 0) {
		return MetastoreProviderType::HMS;
	}
	if (lower.find("thrift+http://") == 0 || lower.find("thrift+https://") == 0) {
		return MetastoreProviderType::HMS;
	}
	if (lower.find("http://") == 0 || lower.find("https://") == 0) {
		// HTTP(S) HMS endpoints are accepted.
		if (lower.find("dataproc") != std::string::npos) {
			return MetastoreProviderType::Dataproc;
		}
		return MetastoreProviderType::HMS;
	}
	if (lower.find("arn:aws:glue:") == 0) {
		return MetastoreProviderType::Glue;
	}
	if (lower.find("https://") == 0 && lower.find("dataproc") != std::string::npos) {
		return MetastoreProviderType::Dataproc;
	}
	return MetastoreProviderType::Unknown;
}

static std::string NormalizeEndpointScheme(const std::string &endpoint) {
	auto lower = StringUtil::Lower(endpoint);
	if (lower.find("thrift+http://") == 0) {
		return "http://" + endpoint.substr(14);
	}
	if (lower.find("thrift+https://") == 0) {
		return "https://" + endpoint.substr(15);
	}
	return endpoint;
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
	auto type_str = GetOptionString(options, "TYPE");
	auto endpoint_str = GetOptionString(options, "ENDPOINT");

	MetastoreConnectorConfig config;

	// TYPE parameter must be 'metastore'
	if (type_str.empty()) {
		throw_metastore_error(
		    MetastoreErrorCode::InvalidConfig, MetastoreErrorTag {"unknown", "ResolveConnectorConfig", false},
		    "TYPE metastore is required. "
		    "Examples: ATTACH 'thrift://hms-host:9083' AS hms (TYPE metastore) or "
		    "ATTACH 'arn:aws:glue:us-east-1:123456789012:catalog' AS glue (TYPE metastore, REGION 'us-east-1')");
	}

	auto lower = StringUtil::Lower(type_str);
	if (lower != "metastore") {
		throw_metastore_error(MetastoreErrorCode::InvalidConfig,
		                      MetastoreErrorTag {"unknown", "ResolveConnectorConfig", false},
		                      "TYPE must be 'metastore', got '" + type_str + "'");
	}

	if (endpoint_str.empty()) {
		throw_metastore_error(MetastoreErrorCode::InvalidConfig,
		                      MetastoreErrorTag {"unknown", "ResolveConnectorConfig", false},
		                      "Endpoint is required in ATTACH path. "
		                      "Example: ATTACH 'thrift://hms-host:9083' AS hms (TYPE metastore)");
	}

	config.provider = InferProviderFromUrl(endpoint_str);
	if (config.provider == MetastoreProviderType::Unknown) {
		throw_metastore_error(MetastoreErrorCode::InvalidConfig,
		                      MetastoreErrorTag {"unknown", "ResolveConnectorConfig", false},
		                      "Could not infer metastore provider from endpoint. "
		                      "Use thrift://, thrift+http(s)://, or http(s):// for HMS, arn:aws:glue: for Glue, or "
		                      "https://...dataproc... for Dataproc.");
	}

	config.endpoint = NormalizeEndpointScheme(endpoint_str);

	auto region_str = GetOptionString(options, "REGION");
	if (!region_str.empty()) {
		config.region = region_str;
	} else if (config.provider == MetastoreProviderType::Glue) {
		auto inferred_region = ExtractGlueRegionFromArn(endpoint_str);
		if (inferred_region.has_value()) {
			config.region = inferred_region.value();
		}
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
		throw_metastore_error(MetastoreErrorCode::InvalidConfig,
		                      MetastoreErrorTag {provider_name, "ResolveConnectorConfig", false},
		                      "Provider '" + std::string(provider_name) + "' is not yet supported.");
	}

	return config;
}

} // namespace duckdb
