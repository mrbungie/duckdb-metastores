#pragma once

#include "auth/metastore_secret_bridge.hpp"

#include <optional>
#include <string>

namespace duckdb {

void RegisterMetastoreAttachConfig(const std::string &catalog_name, MetastoreConnectorConfig config);
std::optional<MetastoreConnectorConfig> LookupMetastoreAttachConfig(const std::string &catalog_name);

}
