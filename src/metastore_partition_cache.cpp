#include "metastore_partition_cache.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>
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

	if (normalized.empty()) {
		return normalized;
	}

	std::vector<std::string> clauses;
	std::string current_clause;
	current_clause.reserve(normalized.size());

	bool split_in_quote = false;
	idx_t paren_depth = 0;
	for (idx_t i = 0; i < normalized.size(); i++) {
		auto ch = normalized[i];
		if (ch == '\'') {
			split_in_quote = !split_in_quote;
			current_clause.push_back(ch);
			continue;
		}

		if (!split_in_quote) {
			if (ch == '(') {
				paren_depth++;
			} else if (ch == ')' && paren_depth > 0) {
				paren_depth--;
			}

			if (paren_depth == 0 && i > 0 && i + 3 < normalized.size() && normalized[i - 1] == ' ' &&
			    normalized[i + 3] == ' ' && std::toupper(static_cast<unsigned char>(normalized[i])) == 'A' &&
			    std::toupper(static_cast<unsigned char>(normalized[i + 1])) == 'N' &&
			    std::toupper(static_cast<unsigned char>(normalized[i + 2])) == 'D') {
				StringUtil::Trim(current_clause);
				clauses.push_back(current_clause);
				current_clause.clear();
				i += 3;
				continue;
			}
		}

		current_clause.push_back(ch);
	}

	StringUtil::Trim(current_clause);
	clauses.push_back(current_clause);

	if (clauses.size() <= 1) {
		return normalized;
	}

	std::sort(clauses.begin(), clauses.end());
	std::string canonical;
	canonical.reserve(normalized.size());
	for (idx_t clause_idx = 0; clause_idx < clauses.size(); clause_idx++) {
		if (clause_idx > 0) {
			canonical += " AND ";
		}
		canonical += clauses[clause_idx];
	}
	return canonical;
}

const std::vector<MetastorePartitionValue> *MetastorePartitionCache::Lookup(const std::string &cache_key,
	                                                                        int64_t ttl_seconds,
	                                                                        int64_t negative_ttl_seconds,
	                                                                        bool stale_read_enabled,
	                                                                        bool *refresh_stale_entry) {
	if (ttl_seconds <= 0) {
		return nullptr;
	}
	auto it = entries_.find(cache_key);
	if (it == entries_.end()) {
		return nullptr;
	}
	auto effective_ttl = ttl_seconds;
	if (it->second.partitions.empty()) {
		effective_ttl = negative_ttl_seconds;
	}
	if (effective_ttl <= 0) {
		lru_keys_.erase(it->second.lru_it);
		entries_.erase(it);
		return nullptr;
	}
	auto age = std::chrono::steady_clock::now() - it->second.inserted_at;
	if (age > std::chrono::seconds(effective_ttl)) {
		if (stale_read_enabled) {
			if (refresh_stale_entry) {
				*refresh_stale_entry = it->second.needs_refresh;
			}
			it->second.needs_refresh = true;
			lru_keys_.erase(it->second.lru_it);
			lru_keys_.push_front(it->first);
			it->second.lru_it = lru_keys_.begin();
			return &it->second.partitions;
		}
		lru_keys_.erase(it->second.lru_it);
		entries_.erase(it);
		return nullptr;
	}
	if (refresh_stale_entry) {
		*refresh_stale_entry = false;
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
		it->second.needs_refresh = false;
		return;
	}
	while (entries_.size() >= max_entries && !lru_keys_.empty()) {
		auto victim = lru_keys_.back();
		lru_keys_.pop_back();
		entries_.erase(victim);
	}
	lru_keys_.push_front(cache_key);
	entries_[cache_key] = {std::move(partitions), std::chrono::steady_clock::now(), lru_keys_.begin(), false};
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
