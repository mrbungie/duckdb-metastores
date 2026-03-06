#include "metastore_scan_plan.hpp"
#include "connector/metastore_connector.hpp"
#include "formats/format_reader.hpp"
#include "metastore_partition_cache.hpp"
#include <algorithm>

namespace duckdb {

// ──────────────────────────────────────────────────────────────
// Stage 0: Partition-name extraction
// Builds sorted, deduplicated "col=val/col=val" strings for display.
// ──────────────────────────────────────────────────────────────
static std::vector<std::string> ExtractPartitionNames(const MetastoreTable &table,
                                                      const std::vector<MetastorePartitionValue> &partitions) {
	std::vector<std::string> names;
	for (auto &part : partitions) {
		std::string name;
		for (idx_t i = 0; i < part.values.size() && i < table.partition_spec.columns.size(); i++) {
			if (!name.empty()) {
				name += "/";
			}
			name += table.partition_spec.columns[i].name + "=" + part.values[i];
		}
		if (!name.empty()) {
			names.push_back(std::move(name));
		}
	}
	sort(names.begin(), names.end());
	names.erase(unique(names.begin(), names.end()), names.end());
	return names;
}

// ──────────────────────────────────────────────────────────────
// Helper: Hidden / sidecar file detection
// Returns true for files whose basename starts with '.' or '_',
// or that end with '.crc'.  Used during file expansion to mirror
// Hive conventions for hidden and checksum sidecar files.
// ──────────────────────────────────────────────────────────────
static bool IsIgnoredFile(const string &file_path) {
	auto slash_pos = file_path.find_last_of("/\\");
	auto file_name = slash_pos == string::npos ? file_path : file_path.substr(slash_pos + 1);
	return StringUtil::StartsWith(file_name, ".") || StringUtil::StartsWith(file_name, "_") ||
	       StringUtil::EndsWith(file_name, ".crc");
}

// ──────────────────────────────────────────────────────────────
// Helper: Normalize a format-reader scan path
// Strips the '[!._]*' suffix injected by some format readers and
// replaces it with a plain '*' glob, ensuring the directory
// separator is present.  Returns empty string if the reader
// returns an empty path (caller should skip).
// ──────────────────────────────────────────────────────────────
static string NormalizeScanPath(const IFormatReader &reader, const string &raw_path) {
	auto path = reader.BuildScanPath(raw_path);
	if (path.empty()) {
		return path;
	}
	// Strip format-reader [!._]* suffix and replace with plain glob
	if (StringUtil::EndsWith(path, "[!._]*")) {
		path = path.substr(0, path.size() - 6);
		if (!StringUtil::EndsWith(path, "/")) {
			path += "/";
		}
		path += "*";
	}
	return path;
}

// ──────────────────────────────────────────────────────────────
// Stage 2: File expansion and filtering
// Given a single raw partition/table path, resolves it to concrete
// file paths via glob expansion, filters out hidden/sidecar files,
// and appends the results to the plan's file list.
//
// Semantics preserved from the original AddResolvedFiles lambda:
//   - Empty paths (from format reader) are silently skipped.
//   - Glob paths are expanded; hidden files are removed.
//   - If glob expansion yields zero files after filtering, the
//     raw glob pattern is kept as a fallback.
//   - If glob expansion throws, the raw path is kept as-is.
//   - Non-glob paths are added directly if not ignored.
// ──────────────────────────────────────────────────────────────
static void ExpandFiles(MetastoreScanPlan &plan, FileSystem &fs, ClientContext &context,
                        const IFormatReader &reader, const string &raw_path, idx_t part_idx) {
	auto path = NormalizeScanPath(reader, raw_path);
	if (path.empty()) {
		return;
	}

	if (FileSystem::HasGlob(path)) {
		try {
			auto expanded = fs.GlobFiles(path, context, FileGlobOptions::ALLOW_EMPTY);
			expanded.erase(std::remove_if(expanded.begin(), expanded.end(),
			                              [&](const auto &file) { return IsIgnoredFile(file.path); }),
			               expanded.end());
			if (expanded.empty()) {
				plan.files.push_back(path);
				plan.file_partition_indices.push_back(part_idx);
			} else {
				for (auto &file : expanded) {
					if (!IsIgnoredFile(file.path)) {
						plan.files.push_back(file.path);
						plan.file_partition_indices.push_back(part_idx);
					}
				}
			}
		} catch (...) {
			plan.files.push_back(path);
			plan.file_partition_indices.push_back(part_idx);
		}
	} else {
		if (!IsIgnoredFile(path)) {
			plan.files.push_back(path);
			plan.file_partition_indices.push_back(part_idx);
		}
	}
}

// ──────────────────────────────────────────────────────────────
// Stage 1: Partition listing and validation
// Fetches partitions from the connector, validates the count
// against the configured limit, and populates the plan with
// partition metadata and display names.
//
// Throws BinderException on connector failure or when partition
// count exceeds max_partitions.
// ──────────────────────────────────────────────────────────────
static void ListPartitions(MetastoreScanPlan &plan, ClientContext &context, IMetastoreConnector &connector,
                           const MetastoreTable &table, const MetastorePlanOptions &opt, int64_t cache_ttl,
                           idx_t cache_max_entries) {
	auto cache = MetastorePartitionCache::GetOrCreate(context);
	auto cache_key = MakePartitionCacheKey(opt.catalog_name, connector.GetNamespace(), opt.table_name, opt.predicate);

	if (cache_ttl > 0) {
		if (auto *cached = cache->Lookup(cache_key, cache_ttl)) {
			plan.partitions_examined = cached->size();
			if (plan.partitions_examined > opt.max_partitions) {
				throw BinderException("Too many partitions (%s) for table %s.%s. Add a simple predicate on partition "
				                      "columns or increase metastore_max_partitions.",
				                      to_string(plan.partitions_examined), connector.GetNamespace(), opt.table_name);
			}
			plan.partitions.assign(cached->begin(), cached->end());
			plan.selected_partitions = ExtractPartitionNames(table, plan.partitions);
			return;
		}
	}

	auto parts_result = connector.ListPartitions(opt.table_name, opt.predicate);
	if (!parts_result.IsOk()) {
		throw BinderException("Failed to list partitions for %s.%s: %s", connector.GetNamespace(), opt.table_name,
		                      parts_result.error.message);
	}

	plan.partitions_examined = parts_result.value.size();
	if (plan.partitions_examined > opt.max_partitions) {
		throw BinderException("Too many partitions (%s) for table %s.%s. Add a simple predicate on partition "
		                      "columns or increase metastore_max_partitions.",
		                      to_string(plan.partitions_examined), connector.GetNamespace(), opt.table_name);
	}

	if (cache_ttl > 0) {
		cache->Insert(cache_key, parts_result.value, cache_max_entries);
	}
	plan.partitions = std::move(parts_result.value);
	plan.selected_partitions = ExtractPartitionNames(table, plan.partitions);
}

// ──────────────────────────────────────────────────────────────
// Stage 3: Scan-target assembly
// For partitioned tables, expands each partition's location into
// concrete file paths.  If no files are found across all partitions,
// falls back to the table's base storage location.
// For non-partitioned tables, expands the table's storage location
// directly.
// ──────────────────────────────────────────────────────────────
static void AssembleScanTargets(MetastoreScanPlan &plan, FileSystem &fs, ClientContext &context,
                                const IFormatReader &reader, const MetastoreTable &table) {
	if (!plan.is_partitioned) {
		ExpandFiles(plan, fs, context, reader, table.storage_descriptor.location, 0);
		return;
	}

	for (idx_t i = 0; i < plan.partitions.size(); i++) {
		ExpandFiles(plan, fs, context, reader, plan.partitions[i].location, i);
	}

	// Fallback: if no partition yielded any files, try the base location
	if (plan.files.empty()) {
		ExpandFiles(plan, fs, context, reader, table.storage_descriptor.location, 0);
	}
}

// ──────────────────────────────────────────────────────────────
// PlanScan – top-level orchestrator
// Delegates to deterministic stages in order:
//   1. ListPartitions   – fetch & validate partition metadata
//   2. AssembleScanTargets – expand each location into files
// PartitionNames (ExtractPartitionNames) is called inside
// ListPartitions.  ExpandFiles + IsIgnoredFile are called
// inside AssembleScanTargets.
// ──────────────────────────────────────────────────────────────
MetastoreScanPlan PlanScan(ClientContext &context, IMetastoreConnector &connector, const MetastoreTable &table,
                           const MetastorePlanOptions &opt) {
	MetastoreScanPlan plan;
	plan.is_partitioned = table.IsPartitioned();
	plan.used_predicate = !opt.predicate.empty();
	plan.partitions_examined = 0;

	auto &fs = FileSystem::GetFileSystem(context);
	auto &reader = GetFormatReader(table.storage_descriptor.format);
	int64_t cache_ttl = 30;
	idx_t cache_max_entries = 256;
	Value ttl_val;
	if (context.TryGetCurrentSetting("metastore_partition_cache_ttl", ttl_val)) {
		cache_ttl = ttl_val.GetValue<int64_t>();
	}
	Value max_entries_val;
	if (context.TryGetCurrentSetting("metastore_partition_cache_max_entries", max_entries_val)) {
		auto configured = max_entries_val.GetValue<int64_t>();
		if (configured < 0) {
			cache_max_entries = 0;
		} else {
			cache_max_entries = UnsafeNumericCast<idx_t>(configured);
		}
	}

	// Stage 1: list and validate partitions (partitioned tables only)
	if (plan.is_partitioned) {
		ListPartitions(plan, context, connector, table, opt, cache_ttl, cache_max_entries);
	}

	// Stage 2: expand partition/table locations into concrete scan files
	AssembleScanTargets(plan, fs, context, reader, table);

	return plan;
}

} // namespace duckdb
