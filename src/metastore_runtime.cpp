#include "metastore_runtime.hpp"

#include "duckdb/common/string_util.hpp"

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace duckdb {

static std::mutex runtime_mutex;
static std::unordered_map<std::string, MetastoreConnectorConfig> runtime_configs;

void RegisterMetastoreAttachConfig(const std::string &catalog_name, MetastoreConnectorConfig config) {
	std::lock_guard<std::mutex> lock(runtime_mutex);
	runtime_configs[StringUtil::Lower(catalog_name)] = std::move(config);
}

std::optional<MetastoreConnectorConfig> LookupMetastoreAttachConfig(const std::string &catalog_name) {
	std::lock_guard<std::mutex> lock(runtime_mutex);
	auto it = runtime_configs.find(StringUtil::Lower(catalog_name));
	if (it == runtime_configs.end()) {
		return std::nullopt;
	}
	return it->second;
}

}
