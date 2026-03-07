#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "metastore_types.hpp"

#include <chrono>
#include <list>
#include <string>
#include <unordered_map>

namespace duckdb {

struct PartitionCacheEntry {
	std::vector<MetastorePartitionValue> partitions;
	std::chrono::steady_clock::time_point inserted_at;
	std::list<std::string>::iterator lru_it;
	bool needs_refresh = false;
};

class MetastorePartitionCache : public ClientContextState {
public:
	static constexpr const char *KEY = "metastore_partition_cache";

	const std::vector<MetastorePartitionValue> *Lookup(const std::string &cache_key, int64_t ttl_seconds,
	                                                  int64_t negative_ttl_seconds, bool stale_read_enabled,
	                                                  bool *refresh_stale_entry = nullptr);

	void Insert(const std::string &cache_key, std::vector<MetastorePartitionValue> partitions, idx_t max_entries);

	void InvalidateTable(const std::string &catalog, const std::string &ns, const std::string &table);

	static shared_ptr<MetastorePartitionCache> GetOrCreate(ClientContext &context);

private:
	void EraseEntry(const std::string &cache_key);
	std::string MakeTablePrefix(const std::string &catalog, const std::string &ns, const std::string &table) const;

	std::unordered_map<std::string, PartitionCacheEntry> entries_;
	std::list<std::string> lru_keys_;
};

std::string MakePartitionCacheKey(const std::string &catalog, const std::string &ns, const std::string &table,
	                              const std::string &predicate);

}
