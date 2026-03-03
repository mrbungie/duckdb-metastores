## 0. Sanity check on APIs

* Does `StorageExtension` only handle `ATTACH` and attachment scoped state, while `Extension` registers functions, replacement scans, settings, etc?

  Answer: Yes. StorageExtension implements the ATTACH path and attachment-scoped state (see src/metastore_storage_extension.cpp: MetastoreAttach and CreateMetastoreStorageExtension). The Extension (MetastoreExtension) registers SQL surface area such as table functions, replacement scans, and extension options (see src/metastore_extension.cpp).

* Are you using `StorageExtension` primarily to own per attached catalog state, and `Extension` to expose SQL surface area?

  Answer: Yes. The code registers the storage extension in DBConfig (config.storage_extensions["metastore"]) for attach behavior and uses Extension::Load to register functions and replacement scans, matching DuckDB conventions.

## 1. Rename or remove `metastore_scan`

* Is `metastore_scan` actually returning one metadata row, rather than scanning data files?

  Answer: Yes — metastore_scan fetches table metadata via connector->GetTable(...) and returns exactly one metadata row (table_catalog, table_schema, table_name, location, format). It is a metadata/debug helper, not a data-scanning primitive.

* Does the name “scan” mislead users and future you into thinking it is a data scan primitive?

  Answer: Yes — the name is misleading. Renaming to `metastore_table_info` or `metastore_describe` would make intent clear.

* Is this function used by anything external, or only as an internal helper?

  Answer: It's used primarily as an internal helper and in tests (see test/sql/metastore/*.test). The canonical data access is handled by metastore_read and the replacement scan.

* If kept, would renaming to `metastore_table_info` or `metastore_describe` better match what it returns?

  Answer: Yes — prefer `metastore_table_info` / `metastore_describe` for clarity.

* If removed, do you still have an equivalent way to debug table metadata from SQL?

  Answer: You would lose the convenient SQL helper; keep it (renamed) unless you have an alternative metadata inspection function.

## 2. Extract a `MetastoreScanPlan`

* Do you currently duplicate planning logic in multiple places (at least bind plus pushdown)?

  Answer: Yes. metastore_read.cpp duplicates partition listing and scan file construction in both MetastoreReadBind and MetastoreReadPushdownComplexFilter. Extracting a plan removes this duplication.

* Is partition listing, file path construction, and partition predicate application currently intertwined with binding code?

  Answer: Yes — binding code currently calls ListPartitions (with empty predicate) and builds scan_files; pushdown repeats the same work with a computed predicate.

* Would a single struct like this capture all the outputs you need?

```cpp
struct MetastoreScanPlan {
    vector<string> files;
    vector<string> selected_partitions;
    bool is_partitioned;
};
```

  Answer: Yes — this struct captures the required planning outputs.

* Can you make one function like `MetastoreScanPlan PlanScan(...)` that is the only place that, what would that need?:

  Answer: Yes. PlanScan should accept the connector (IMetastoreConnector), table metadata, and an optional predicate string, then:
  - list partitions via connector->ListPartitions(schema, table, predicate)
  - prune partitions using the predicate
  - build file paths via MetastoreUtils::BuildScanPath
  - return MetastoreScanPlan

* Would both `MetastoreReadBind` and `MetastoreReadPushdownComplexFilter` be able to call `PlanScan(...)` without needing to know provider specifics?

  Answer: Yes. The provider abstraction already hides provider specifics; PlanScan should operate against IMetastoreConnector and canonical types.

## 3. Stop always listing all partitions in bind

* Do you currently call `ListPartitions(schema, table, "")` unconditionally during bind, even when there are filters?

  Answer: Yes — MetastoreReadBind currently calls ListPartitions(schema, table_name, "") for partitioned tables, which enumerates all partitions at bind time.

* Does bind run early enough (and often enough) that this becomes a big cost for large partitioned tables?

  Answer: Yes — bind occurs before pushdown and can be expensive for large partition counts. Avoiding unconditional enumeration is recommended.

* Do you already have filter information available at bind time in your table function bind path, or is it only available later in pushdown?

  Answer: Filter information is only available later (pushdown phase). The binder does not have query filters in the table function bind inputs.

* If filters exist, can you translate them immediately into a partition predicate, so you only list relevant partitions?

  Answer: Not at bind time. Instead: either defer partition listing to pushdown/init or run PlanScan with empty predicate at bind and re-run with a predicate during pushdown; if no pushdown happens, enumerate in init/execute as a fallback.

* When partition pruning is not possible (complex predicates, non equality, unknown expressions), do you fall back to full listing safely?

  Answer: Current code falls back to full listing when no predicate is provided. Add guardrails such as maximum partition thresholds and clear errors/warnings for huge enumerations.

* Do you have guardrails like:

  * maximum partitions to enumerate
  * early exit
  * clear error message or warning when enumeration is massive

  Answer: No explicit guardrails exist today. Add a max-partition threshold and user-visible warning/error.

## 4. Avoid rebinding the underlying function on every pushdown

* In your current flow, does pushdown update `scan_files` and then call `BindUnderlyingFunction` again each time?

  Answer: Yes — MetastoreReadPushdownComplexFilter updates bind_data.scan_files and calls BindUnderlyingFunction(context, bind_data), rebinding the underlying table function.

* Is that rebinding happening for projection pushdown too, or only complex filter pushdown?

  Answer: The code calls BindUnderlyingFunction in the complex filter pushdown path. Projection pushdown is supported by the table function flags but rebinds are triggered by scan_files updates.

* Is rebinding measurably expensive for typical workloads (many pushdowns, repeated bind calls)?

  Answer: It can be; rebinding does catalog lookup, validation, and bind work. Centralizing or avoiding repeated rebinding reduces overhead.

* Short term: can you centralize the rebinding logic so the cost is at least controlled and easier to optimize later?

  Answer: Yes — centralize PlanScan + BindUnderlyingFunction so rebinding is a controlled step.

* Long term (parquet case): can you follow the Iceberg style pattern:

  * clone `parquet_scan`
  * inject a custom `MultiFileReader`
  * update the file list without rebinding

  Answer: Yes — implement parquet-specific MultiFileReader and avoid rebinding for parquet; keep JSON/CSV as simpler paths.

* If you do the Iceberg pattern, is it isolated to parquet only, leaving JSON and CSV delegated paths unchanged?

  Answer: Yes — isolate the complexity to parquet.

## 5. Delete or fix `ResolveScanFiles`

* Does `ResolveScanFiles` rely on `std::filesystem` assumptions that break with remote URIs (s3, gs, azure, http)?

  Answer: Yes — ResolveScanFiles uses std::filesystem::directory_iterator and exists, which only work for local files. Remote URIs need provider-specific expansion or must be skipped.

* Is it currently unused, dead code, or only used in local dev paths?

  Answer: It is used in the bind path when local glob markers are present. It's not dead code but must be guarded.

* If it is needed, can you isolate it behind a “local only” path check, and never call it for URIs with a scheme?

  Answer: Yes — check for a scheme ("://") and only run filesystem-based expansion for local paths.

* If it is not needed, can you delete it to prevent future subtle bugs?

  Answer: If local glob expansion is unnecessary, remove it. Otherwise, restrict it to local paths.

## 6. Keep directory structure mostly as is

* Is your current folder layout already close to the Iceberg extension style, meaning small, cohesive files and minimal subfolder sprawl? Yes

  Answer: Yes — layout is already cohesive (functions/, providers/, include/), and matches the small-file Iceberg style.

* Would adding only `metastore_scan_plan.*` be enough to introduce the new boundary without a refactor wave?

  Answer: Yes — adding metastore_scan_plan.hpp/.cpp and moving partition planning there is surgical and minimal.

* Can you slim down `metastore_read.cpp` by moving planning logic out, while leaving registration and DuckDB glue in place?

  Answer: Yes — move PartitionNames, ResolveScanFiles (guarded or removed), ListPartitions-based file construction into PlanScan; leave bind/pushdown glue and BindUnderlyingFunction in metastore_read.cpp.

## 7. Future parquet optimization

* Do you actually need maximum performance now, or is correctness and architecture the priority? Correctness then performance

  Answer: Correctness and clean architecture first; profile and optimize parquet later with a MultiFileReader.

* Would this confine performance complexity to parquet only, without contaminating the planner with file reader details? It should be an scalable way to make this without overcomplexity.

  Answer: Yes — implement parquet-specific optimizations behind a format-specific code path after extracting PlanScan.

## Final mental model after changes

* Does the provider layer do only provider IO and mapping (HMS connector, later Glue connector), and return your canonical metastore types?

  Answer: Yes — connectors (e.g., HmsConnector) already return canonical MetastoreTable/MetastorePartition types and encapsulate provider logic.

* Does the planner layer expose one primary entry point, `PlanScan()`, returning a `MetastoreScanPlan`?

  Answer: Yes — PlanScan(connector, table, optional_predicate) should centralize partition listing, pruning, and file resolution.

* Does execution bind delegate to underlying scan (parquet, json, csv), forward pushdown and projection, and rely on the scan plan outputs?

  Answer: Yes — keep BindUnderlyingFunction and execution delegation; use PlanScan outputs as input.

* Does this avoid a massive refactor and avoid unnecessary abstraction while still giving a clean separation and large pruning win?

  Answer: Yes — the extraction is minimal and surgical: provider and execution layers remain unchanged while planning is centralized.

If you want, paste just the signatures and call sites for:

* `MetastoreReadBind`
* `MetastoreReadPushdownComplexFilter`
* your current `ListPartitions` usage

I can then rewrite the exact `PlanScan(...)` signature and where it should live, so the diff stays small.

-- Implementation notes / next steps --

- Add files: `src/include/metastore_scan_plan.hpp`, `src/metastore_scan_plan.cpp` implementing:
  - MetastoreScanPlan struct
  - MetastoreScanPlan PlanScan(optional_ptr<IMetastoreConnector> connector,
                                const MetastoreTable &table,
                                const std::string &predicate /*empty => list all*/);

- Change MetastoreReadBind to call PlanScan (or defer PlanScan until init if partitioned and no predicate available). Change MetastoreReadPushdownComplexFilter to call PlanScan with computed predicate and only rebind there.

- Guard or remove ResolveScanFiles: only use std::filesystem for local paths (no scheme), otherwise skip glob expansion.

- Add guardrail: max partition enumeration threshold with clear error/warning.
