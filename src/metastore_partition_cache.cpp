#include "metastore_partition_cache.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <cctype>

namespace duckdb {

static std::string NormalizePredicateForCache(const std::string &predicate) {
	std::string normalized;
	normalized.reserve(predicate.size());
	bool in_quote = false;
	bool pending_space = false;
	for (auto ch : predicate) {
		if (ch == '\'') {
			if (pending_space && !normalized.empty() && normalized.back() != ' ') {
				normalized.push_back(' ');
			}
			pending_space = false;
			in_quote = !in_quote;
			normalized.push_back(ch);
			continue;
		}
		if (!in_quote && std::isspace(static_cast<unsigned char>(ch))) {
			pending_space = true;
			continue;
		}
		if (pending_space && !normalized.empty() && normalized.back() != ' ') {
			normalized.push_back(' ');
		}
		pending_space = false;
		normalized.push_back(ch);
	}
	StringUtil::Trim(normalized);
	return normalized;
}

const std::vector<MetastorePartitionValue> *MetastorePartitionCache::Lookup(const std::string &cache_key,
	                                                                        int64_t ttl_seconds) {
	if (ttl_seconds <= 0) {
		return nullptr;
	}
	auto it = entries_.find(cache_key);
	if (it == entries_.end()) {
		return nullptr;
	}
	auto age = std::chrono::steady_clock::now() - it->second.inserted_at;
	if (age > std::chrono::seconds(ttl_seconds)) {
		lru_keys_.erase(it->second.lru_it);
		entries_.erase(it);
		return nullptr;
	}
	lru_keys_.erase(it->second.lru_it);
	lru_keys_.push_front(it->first);
	it->second.lru_it = lru_keys_.begin();
	return &it->second.partitions;
}

void MetastorePartitionCache::Insert(const std::string &cache_key,
	                                 std::vector<MetastorePartitionValue> partitions, idx_t max_entries) {
	if (max_entries == 0) {
		return;
	}
	auto it = entries_.find(cache_key);
	if (it != entries_.end()) {
		lru_keys_.erase(it->second.lru_it);
		lru_keys_.push_front(cache_key);
		it->second.partitions = std::move(partitions);
		it->second.inserted_at = std::chrono::steady_clock::now();
		it->second.lru_it = lru_keys_.begin();
		return;
	}
	while (entries_.size() >= max_entries && !lru_keys_.empty()) {
		auto victim = lru_keys_.back();
		lru_keys_.pop_back();
		entries_.erase(victim);
	}
	lru_keys_.push_front(cache_key);
	entries_[cache_key] = {std::move(partitions), std::chrono::steady_clock::now(), lru_keys_.begin()};
}

void MetastorePartitionCache::InvalidateTable(const std::string &catalog, const std::string &ns,
	                                          const std::string &table) {
	auto prefix = MakeTablePrefix(catalog, ns, table);
	std::vector<std::string> to_remove;
	to_remove.reserve(entries_.size());
	for (auto &entry : entries_) {
		if (StringUtil::StartsWith(entry.first, prefix)) {
			to_remove.push_back(entry.first);
		}
	}
	for (auto &key : to_remove) {
		EraseEntry(key);
	}
}

shared_ptr<MetastorePartitionCache> MetastorePartitionCache::GetOrCreate(ClientContext &context) {
	return context.registered_state->GetOrCreate<MetastorePartitionCache>(KEY);
}

void MetastorePartitionCache::EraseEntry(const std::string &cache_key) {
	auto it = entries_.find(cache_key);
	if (it == entries_.end()) {
		return;
	}
	lru_keys_.erase(it->second.lru_it);
	entries_.erase(it);
}

std::string MetastorePartitionCache::MakeTablePrefix(const std::string &catalog, const std::string &ns,
	                                                  const std::string &table) const {
	return catalog + "::" + ns + "::" + table + "::";
}

std::string MakePartitionCacheKey(const std::string &catalog, const std::string &ns, const std::string &table,
	                              const std::string &predicate) {
	return catalog + "::" + ns + "::" + table + "::" + NormalizePredicateForCache(predicate);
}

}
