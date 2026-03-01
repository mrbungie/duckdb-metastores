#pragma once

#include "metastore_errors.hpp"
#include "duckdb.hpp"

#include <optional>
#include <string>
#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MetastoreProviderType - supported metastore backend providers
//===--------------------------------------------------------------------===//
enum class MetastoreProviderType : uint8_t { HMS = 0, Glue = 1, Dataproc = 2, Unknown = 255 };

inline const char *MetastoreProviderTypeToString(MetastoreProviderType type) {
	switch (type) {
	case MetastoreProviderType::HMS:
		return "HMS";
	case MetastoreProviderType::Glue:
		return "Glue";
	case MetastoreProviderType::Dataproc:
		return "Dataproc";
	case MetastoreProviderType::Unknown:
	default:
		return "Unknown";
	}
}

//===--------------------------------------------------------------------===//
// MetastoreConnectorConfig - normalized config resolved from ATTACH options
//
// This is the single chokepoint where ATTACH/scan options are mapped to
// connector configuration. No provider adapter should ever touch secrets
// or raw options directly — everything flows through ResolveConnectorConfig().
//===--------------------------------------------------------------------===//
struct MetastoreConnectorConfig {
	//! Which provider backend to use
	MetastoreProviderType provider = MetastoreProviderType::Unknown;
	//! Metastore endpoint URI (e.g. "thrift://hms-host:9083" for HMS)
	std::string endpoint;
	//! Cloud region (required for Glue/Dataproc, optional for HMS)
	std::optional<std::string> region;
	//! Auth strategy class name (e.g. "StaticKeys", "Chain", "AssumeRole")
	std::string auth_strategy_class;
	//! Extensible key-value map for provider-specific parameters
	std::unordered_map<std::string, std::string> extra_params;
};

//===--------------------------------------------------------------------===//
// InferProviderType - case-insensitive string to enum mapping
//===--------------------------------------------------------------------===//
//! Infer the provider type from the PROVIDER option string.
//! Returns MetastoreProviderType::Unknown if the string is unrecognized.
MetastoreProviderType InferProviderType(const std::string &provider_str);

//===--------------------------------------------------------------------===//
// ResolveConnectorConfig - the ONE AND ONLY auth normalization chokepoint
//===--------------------------------------------------------------------===//
//! Resolve a MetastoreConnectorConfig from DuckDB ATTACH options.
//!
//! Reads PROVIDER, ENDPOINT, REGION, SECRET, and AUTH_STRATEGY from the
//! options map. Validates required fields per provider:
//!   - HMS: ENDPOINT required
//!   - Glue: REGION required
//!   - Dataproc: ENDPOINT required
//!
//! If SECRET is present, resolves credentials via DuckDB SecretManager
//! (stubbed until submodule API is available).
//!
//! Throws MetastoreException with MetastoreErrorCode::InvalidConfig on
//! missing or invalid configuration.
MetastoreConnectorConfig ResolveConnectorConfig(const case_insensitive_map_t<Value> &options);

} // namespace duckdb
