#pragma once

#include "auth/metastore_secret_bridge.hpp"

#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/common/string_util.hpp>

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MetastoreStorageInfo
//
// Holds the per-DatabaseInstance catalog-name → connector config map.
// Stored as StorageExtension::storage_info so that each DatabaseInstance
// has its own isolated map. Avoids the process-global state antipattern.
//===--------------------------------------------------------------------===//
struct MetastoreStorageInfo : public StorageExtensionInfo {
	//! Register a connector config for a newly attached catalog.
	void Register(const std::string &catalog_name, MetastoreConnectorConfig config) {
		std::lock_guard<std::mutex> lock(mutex_);
		configs_[StringUtil::Lower(catalog_name)] = std::move(config);
	}

	//! Look up the connector config for an attached catalog.
	std::optional<MetastoreConnectorConfig> Lookup(const std::string &catalog_name) const {
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = configs_.find(StringUtil::Lower(catalog_name));
		if (it == configs_.end()) {
			return std::nullopt;
		}
		return it->second;
	}

private:
	mutable std::mutex mutex_;
	std::unordered_map<std::string, MetastoreConnectorConfig> configs_;
};

} // namespace duckdb
