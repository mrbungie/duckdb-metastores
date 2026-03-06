#pragma once

#include "metastore_types.hpp"
#include "connector/metastore_errors.hpp"
#include "duckdb.hpp"

#include <optional>
#include <string>
#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// InferProviderType - case-insensitive std::string to enum mapping
//===--------------------------------------------------------------------===//
//! Infer the provider type from the PROVIDER option std::string.
//! Returns MetastoreProviderType::Unknown if the std::string is unrecognized.
MetastoreProviderType InferProviderType(const std::string &provider_str);

//===--------------------------------------------------------------------===//
// ResolveConnectorConfig - the ONE AND ONLY auth normalization chokepoint
//===--------------------------------------------------------------------===//
//! Resolve a MetastoreCatalogConfig from DuckDB ATTACH options.
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
MetastoreCatalogConfig ResolveConnectorConfig(const case_insensitive_map_t<Value> &options);

} // namespace duckdb
