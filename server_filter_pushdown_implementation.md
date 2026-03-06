# Server-Side Filter Pushdown Implementation Plan

Three targeted fixes to make the DuckDB metastore extension efficient at partition filtering and pushdown. No behavior changes to existing working paths. HMS only (Glue skipped).

---

## Context

The predicate plumbing is already 90% in place:

| Layer | Status |
|---|---|
| Predicate string built from DuckDB `TableFilter` | ✅ `MetastorePartitionPredicate::FromTableFilters` in `metastore_partition_predicate.cpp` |
| Predicate passed down to connector | ✅ `MetastorePlanOptions::predicate` → `connector.ListPartitions(name, predicate)` in `metastore_scan_plan.cpp:129` |
| HMS connector actually uses it | ❌ `(void)predicate;` on line 244 of `hms_connector.cpp` |
| Partition list caching | ❌ entirely absent |
| Typed range comparison in client-side pruning | ❌ raw string `>`, `<` etc. in `EvaluatePartitionValueFilter` |

---

## Fix 1 — HMS Server-Side Partition Filter Pushdown

**File:** `src/providers/hms/hms_connector.cpp`

### What to change

Replace the existing `ListPartitions` body (lines 242–297) so that when `predicate` is non-empty, the connector calls `get_partitions_by_filter` instead of `get_partition_names`. Fall back to `get_partition_names` on any exception (supports older HMS deployments).

`get_partitions_by_filter` returns `std::vector<Apache::Hadoop::Hive::Partition>` where each `Partition` has:
- `values` — `std::vector<std::string>` (one entry per partition column, in spec order)
- `sd.location` — `std::string` (the physical location for that partition)

`get_partition_names` returns `std::vector<std::string>` in `col=val/col=val` format, which is then parsed by the existing `ParsePartitionNameValues` helper and location is reconstructed from the table base location.

### Current code (lines 242–297)

```cpp
MetastoreResult<std::vector<MetastorePartitionValue>> HmsConnector::ListPartitions(const std::string &table_name,
                                                                                   const std::string &predicate) {
    (void)predicate;

    auto conn_res = ConnectHms(config_);
    if (!conn_res.IsOk()) { ... }

    std::vector<std::string> partition_names;
    try {
        conn_res.value.client->get_partition_names(partition_names, bound_namespace_, table_name, -1);
    } catch (const NoSuchObjectException &) {
        partition_names.clear();
    } catch (const MetaException &e) { ... }
      catch (const TException &tx) { ... }

    auto table_result = GetTable(table_name);
    ...
    // reconstructs location from table_location + "/" + name
    // builds MetastorePartitionValue{values, location} for each name
}
```

### Target code structure

```cpp
MetastoreResult<std::vector<MetastorePartitionValue>> HmsConnector::ListPartitions(const std::string &table_name,
                                                                                   const std::string &predicate) {
    auto conn_res = ConnectHms(config_);
    if (!conn_res.IsOk()) { /* existing error return */ }

    // --- Path A: server-side filter via get_partitions_by_filter ---
    if (!predicate.empty()) {
        try {
            std::vector<Apache::Hadoop::Hive::Partition> hms_parts;
            conn_res.value.client->get_partitions_by_filter(
                hms_parts, bound_namespace_, table_name, predicate, -1);

            std::vector<MetastorePartitionValue> result;
            result.reserve(hms_parts.size());
            for (auto &hp : hms_parts) {
                MetastorePartitionValue pv;
                pv.values   = hp.values;          // already split, no parsing needed
                pv.location = NormalizeFileLocation(hp.sd.location);
                result.push_back(std::move(pv));
            }
            return MetastoreResult<std::vector<MetastorePartitionValue>>::Success(std::move(result));

        } catch (const TException &) {
            // Fall through to Path B (full list) on any HMS error
        }
    }

    // --- Path B: full list via get_partition_names (existing logic) ---
    std::vector<std::string> partition_names;
    try {
        conn_res.value.client->get_partition_names(partition_names, bound_namespace_, table_name, -1);
    } catch (const NoSuchObjectException &) {
        partition_names.clear();
    } catch (const MetaException &e) { /* existing error return */ }
      catch (const TException &tx)   { /* existing error return */ }

    // ... rest of existing code: GetTable, DiscoverLocalPartitionNames, reconstruct locations
}
```

### Key notes

- `NormalizeFileLocation` is already defined in the anonymous namespace at the top of the file (line 84) — use it directly.
- The `Apache::Hadoop::Hive::Partition` using-declaration must be added at the top of the file (or use fully qualified name). `Table`, `MetaException`, etc. are already imported via `using` in the anonymous namespace — add `Partition` there.
- The `get_partitions_by_filter` signature (from generated Thrift header line 131):
  ```cpp
  void get_partitions_by_filter(
      std::vector<Partition> &_return,
      const std::string &db_name,
      const std::string &tbl_name,
      const std::string &filter,
      const int16_t max_parts);
  ```
- On the fallback path, behavior is **identical** to the current code — no changes needed there.

---

## Fix 2 — Partition List Caching

**Files:**
- `src/include/metastore_partition_cache.hpp` *(new)*
- `src/metastore_partition_cache.cpp` *(new)*
- `src/metastore_scan_plan.cpp` — hook cache into `ListPartitions` stage
- `src/metastore_extension.cpp` — register `metastore_partition_cache_ttl` option
- `CMakeLists.txt` — add the new .cpp source

### Cache design

```
Key:   catalog_name + "::" + namespace + "::" + table_name + "::" + predicate
Value: vector<MetastorePartitionValue>  +  steady_clock::time_point (insert time)
TTL:   read from "metastore_partition_cache_ttl" setting (seconds, default 30)
       Within a single query plan (same predicate, same bind_data) the result is
       always used from cache — i.e. the within-query guarantee is a natural
       consequence of the existing `last_predicate` dedup in MetastoreReadPushdownComplexFilter.
Scope: per DuckDB session (ClientContext), via registered_state
```

### New header: `src/include/metastore_partition_cache.hpp`

```cpp
#pragma once
#include "metastore_types.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <chrono>
#include <string>
#include <unordered_map>

namespace duckdb {

struct PartitionCacheEntry {
    std::vector<MetastorePartitionValue> partitions;
    std::chrono::steady_clock::time_point inserted_at;
};

class MetastorePartitionCache : public ClientContextState {
public:
    static constexpr const char *KEY = "metastore_partition_cache";

    // Returns nullptr on cache miss or TTL expiry.
    const std::vector<MetastorePartitionValue> *
    Lookup(const std::string &cache_key, int64_t ttl_seconds) const;

    void Insert(const std::string &cache_key,
                std::vector<MetastorePartitionValue> partitions);

    // Retrieve or create the cache for this session.
    static shared_ptr<MetastorePartitionCache>
    GetOrCreate(ClientContext &context);

private:
    std::unordered_map<std::string, PartitionCacheEntry> entries_;
};

// Compose a cache lookup key.
std::string MakePartitionCacheKey(const std::string &catalog,
                                  const std::string &ns,
                                  const std::string &table,
                                  const std::string &predicate);

} // namespace duckdb
```

### New source: `src/metastore_partition_cache.cpp`

```cpp
#include "metastore_partition_cache.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

const std::vector<MetastorePartitionValue> *
MetastorePartitionCache::Lookup(const std::string &cache_key, int64_t ttl_seconds) const {
    auto it = entries_.find(cache_key);
    if (it == entries_.end()) {
        return nullptr;
    }
    if (ttl_seconds > 0) {
        auto age = std::chrono::steady_clock::now() - it->second.inserted_at;
        if (age > std::chrono::seconds(ttl_seconds)) {
            return nullptr;  // expired (entry stays until next insert, not a problem)
        }
    }
    return &it->second.partitions;
}

void MetastorePartitionCache::Insert(const std::string &cache_key,
                                     std::vector<MetastorePartitionValue> partitions) {
    entries_[cache_key] = {std::move(partitions), std::chrono::steady_clock::now()};
}

shared_ptr<MetastorePartitionCache>
MetastorePartitionCache::GetOrCreate(ClientContext &context) {
    return context.registered_state->GetOrCreate<MetastorePartitionCache>(KEY);
}

std::string MakePartitionCacheKey(const std::string &catalog,
                                  const std::string &ns,
                                  const std::string &table,
                                  const std::string &predicate) {
    return catalog + "::" + ns + "::" + table + "::" + predicate;
}

} // namespace duckdb
```

### Extension option: `src/metastore_extension.cpp`

Add after the existing `metastore_max_partitions` option (line 24–25):

```cpp
config.AddExtensionOption("metastore_partition_cache_ttl",
                          "Partition list cache TTL in seconds (0 = disabled)",
                          LogicalType::BIGINT, Value::BIGINT(30));
```

### Hook into scan plan: `src/metastore_scan_plan.cpp`

`PlanScan` receives a `ClientContext &context`. The `ListPartitions` static helper also receives `opt` which currently has `table_name` and `predicate` — but not the catalog or namespace. The connector knows the namespace (`connector.GetNamespace()`), but the catalog name needs to flow in.

**Option A (simpler):** Add a `catalog_name` field to `MetastorePlanOptions` (already in `metastore_scan_plan.hpp`) and set it at the call sites in `metastore_read.cpp`. Then the cache key uses `opt.catalog_name + "::" + connector.GetNamespace() + "::" + opt.table_name + "::" + opt.predicate`.

**Option B:** Use just `namespace + "::" + table + "::" + predicate` (no catalog). Acceptable if a session only ever connects to one catalog (common case), but not strictly correct for multi-catalog sessions.

**Recommendation: Option A.** Add `string catalog_name;` to `MetastorePlanOptions` and populate it at the two call sites in `MetastoreReadPushdownComplexFilter` and `EnsureScanPlanned` (both already set `opt.table_name = bind_data.table_name` — add `opt.catalog_name = bind_data.catalog` alongside).

Modified `ListPartitions` stage in `metastore_scan_plan.cpp`:

```cpp
// New signature (context and ttl added):
static void ListPartitions(MetastoreScanPlan &plan, ClientContext &context,
                           IMetastoreConnector &connector,
                           const MetastoreTable &table,
                           const MetastorePlanOptions &opt,
                           int64_t cache_ttl) {
    // --- Cache lookup ---
    auto cache = MetastorePartitionCache::GetOrCreate(context);
    auto cache_key = MakePartitionCacheKey(
        opt.catalog_name, connector.GetNamespace(), opt.table_name, opt.predicate);

    if (auto *cached = cache->Lookup(cache_key, cache_ttl)) {
        plan.partitions_examined = cached->size();
        if (plan.partitions_examined > opt.max_partitions) {
            throw BinderException("Too many partitions (%s) for table %s.%s ...",
                                  to_string(plan.partitions_examined),
                                  connector.GetNamespace(), opt.table_name);
        }
        plan.partitions = *cached;
        plan.selected_partitions = ExtractPartitionNames(table, plan.partitions);
        return;
    }

    // --- Cache miss: fetch from connector ---
    auto parts_result = connector.ListPartitions(opt.table_name, opt.predicate);
    if (!parts_result.IsOk()) {
        throw BinderException("Failed to list partitions for %s.%s: %s",
                              connector.GetNamespace(), opt.table_name,
                              parts_result.error.message);
    }

    plan.partitions_examined = parts_result.value.size();
    if (plan.partitions_examined > opt.max_partitions) {
        throw BinderException("Too many partitions (%s) for table %s.%s ...",
                              to_string(plan.partitions_examined),
                              connector.GetNamespace(), opt.table_name);
    }

    cache->Insert(cache_key, parts_result.value);          // populate cache
    plan.partitions = std::move(parts_result.value);
    plan.selected_partitions = ExtractPartitionNames(table, plan.partitions);
}
```

`PlanScan` reads the TTL and passes it down:

```cpp
MetastoreScanPlan PlanScan(ClientContext &context, IMetastoreConnector &connector,
                           const MetastoreTable &table, const MetastorePlanOptions &opt) {
    // ... existing setup ...

    // Read cache TTL from session setting
    int64_t cache_ttl = 30;
    Value ttl_val;
    if (context.TryGetCurrentSetting("metastore_partition_cache_ttl", ttl_val)) {
        cache_ttl = ttl_val.GetValue<int64_t>();
    }

    if (plan.is_partitioned) {
        ListPartitions(plan, context, connector, table, opt, cache_ttl);
    }
    // ... existing AssembleScanTargets ...
}
```

### `MetastorePlanOptions` addition (`src/include/metastore_scan_plan.hpp`)

```cpp
struct MetastorePlanOptions {
    string catalog_name;          // ← ADD THIS
    string table_name;
    string predicate;
    idx_t max_partitions = 100000;
    bool allow_expand_paths = false;
};
```

### Call sites in `metastore_read.cpp`

Both `MetastoreReadPushdownComplexFilter` (line ~892) and `EnsureScanPlanned` (line ~944) set `opt.table_name = bind_data.table_name`. Add:

```cpp
opt.catalog_name = bind_data.catalog;
```

### `CMakeLists.txt`

Find where other `src/*.cpp` files are listed and add:

```cmake
src/metastore_partition_cache.cpp
```

---

## Fix 3 — Typed Partition Value Comparison

**File:** `src/functions/metastore_read.cpp`

### Problem

`EvaluatePartitionValueFilter` (line 320) takes `const string &value` and for range comparisons (`>`, `>=`, `<`, `<=`) does raw string comparison. This is wrong for non-string partition columns (INT, DATE, BIGINT, etc.). Example: partition values `"9"` and `"10"` — string ordering gives `"9" > "10"` but numeric ordering gives `9 < 10`.

### Solution

Add `const LogicalType &col_type` parameter to `EvaluatePartitionValueFilter`. For `CONSTANT_COMPARISON` range ops, cast the string partition value to the column's declared type (using `Value(value).DefaultCastAs(col_type)`), then compare against the already-typed `ConstantFilter::constant` using `Value` comparison operators. Fall back to string comparison if the cast fails (malformed partition value).

For `COMPARE_EQUAL` and `COMPARE_NOTEQUAL`, the existing string comparison is safe (HMS stores equality-comparable string representations), so no cast is needed there. For range ops, casting is required for correctness.

`CONJUNCTION_AND` / `CONJUNCTION_OR` recurse — pass `col_type` through to children.

`IN_FILTER` compares value == candidate.ToString() — this is also a string comparison. Apply the same cast approach for safety.

### Modified signature

```cpp
static bool EvaluatePartitionValueFilter(const string &value,
                                         const TableFilter &filter,
                                         const LogicalType &col_type);
```

### Modified body (CONSTANT_COMPARISON range ops only)

```cpp
case ExpressionType::COMPARE_GREATERTHAN:
case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
case ExpressionType::COMPARE_LESSTHAN:
case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
    // Attempt typed comparison
    try {
        Value lhs = Value(value).DefaultCastAs(col_type);
        const Value &rhs = cmp.constant;  // already typed
        switch (cmp.comparison_type) {
        case ExpressionType::COMPARE_GREATERTHAN:
            return lhs > rhs;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            return lhs >= rhs;
        case ExpressionType::COMPARE_LESSTHAN:
            return lhs < rhs;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            return lhs <= rhs;
        default:
            return true;
        }
    } catch (...) {
        // Cast failed (malformed partition value) — fall back to string comparison
        auto rhs = cmp.constant.ToString();
        switch (cmp.comparison_type) {
        case ExpressionType::COMPARE_GREATERTHAN:          return value > rhs;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return value >= rhs;
        case ExpressionType::COMPARE_LESSTHAN:             return value < rhs;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:    return value <= rhs;
        default:                                            return true;
        }
    }
}
```

### Call site update

`EvaluatePartitionValueFilter` is only called from `EvaluateTableFilterKeepVector` (line 463):

```cpp
// Current:
if (!EvaluatePartitionValueFilter(partitions[p].values[part_idx], *entry.second)) {

// Updated: pass the declared type of the partition column
const LogicalType &col_type = table.partition_spec.columns[part_idx].type_logical;
if (!EvaluatePartitionValueFilter(partitions[p].values[part_idx], *entry.second, col_type)) {
```

**Note:** `MetastorePartitionColumn` has a `type` field (string) and may also have a `type_logical` field (LogicalType). Verify the exact field name in `src/include/metastore_types.hpp`. If only `type` (string) is available, use `TransformStringToLogicalType(col.type)` or the equivalent DuckDB API to convert it first.

Check `MetastorePartitionColumn` definition in `src/include/metastore_types.hpp` and confirm whether a `LogicalType` field exists or needs to be derived from the string type at call time.

Also update the recursive calls inside `CONJUNCTION_AND` and `CONJUNCTION_OR`:

```cpp
case TableFilterType::CONJUNCTION_AND: {
    auto &and_filter = filter.Cast<ConjunctionAndFilter>();
    for (auto &child : and_filter.child_filters) {
        if (!EvaluatePartitionValueFilter(value, *child, col_type)) {  // ← add col_type
            return false;
        }
    }
    return true;
}
case TableFilterType::CONJUNCTION_OR: {
    auto &or_filter = filter.Cast<ConjunctionOrFilter>();
    for (auto &child : or_filter.child_filters) {
        if (EvaluatePartitionValueFilter(value, *child, col_type)) {   // ← add col_type
            return true;
        }
    }
    return false;
}
```

---

## Implementation Order

1. **Fix 3 first** — self-contained, single file, easy to verify in isolation
2. **Fix 1 second** — self-contained, single file, needs HMS to test
3. **Fix 2 last** — touches the most files, depends on understanding call sites already confirmed in Fixes 1 & 3

---

## Files Modified Summary

| File | Change |
|---|---|
| `src/providers/hms/hms_connector.cpp` | Fix 1: use `get_partitions_by_filter` when predicate non-empty; fallback to `get_partition_names` |
| `src/functions/metastore_read.cpp` | Fix 3: typed cast in `EvaluatePartitionValueFilter`; Fix 2: add `opt.catalog_name = bind_data.catalog` at two call sites |
| `src/include/metastore_scan_plan.hpp` | Fix 2: add `catalog_name` field to `MetastorePlanOptions` |
| `src/metastore_scan_plan.cpp` | Fix 2: read TTL setting, pass cache through `ListPartitions` |
| `src/metastore_extension.cpp` | Fix 2: register `metastore_partition_cache_ttl` option |
| `src/include/metastore_partition_cache.hpp` | Fix 2: new cache class header |
| `src/metastore_partition_cache.cpp` | Fix 2: new cache class implementation |
| `CMakeLists.txt` | Fix 2: add `src/metastore_partition_cache.cpp` to sources |

---

## Verification Checklist

- [ ] `make release` exits 0
- [ ] `./build/release/test/unittest "[sql]"` passes (all generic SQL tests)
- [ ] `lsp_diagnostics` clean on all modified files
- [ ] HMS integration test: query with partition predicate returns correct rows and issues only one HMS RPC per unique predicate within a query (observable via `metastore_debug` flag if wired, or HMS logs)
- [ ] Verify typed comparison: table with INT partition column `p`, query `WHERE p > 9` with values `["1","2","9","10","11"]` returns `["10","11"]` not `["9"]` (string order would incorrectly include `"9"` and exclude `"10"`, `"11"` depending on HMS vs client pruning path)
