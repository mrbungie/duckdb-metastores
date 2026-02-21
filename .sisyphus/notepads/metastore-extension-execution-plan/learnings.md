# Learnings

## [2026-02-21] Session ses_37ef4b4c7ffe4M2FY7wrURNAb3 - Wave 1 Start

### Repo Baseline

- Extension template uses **quack** as the current extension name in all files
- Key files to rename/replace:
  - `src/quack_extension.cpp` → `src/metastore_extension.cpp`
  - `src/include/quack_extension.hpp` → `src/include/metastore_extension.hpp`
  - `CMakeLists.txt` sets `TARGET_NAME quack` → must change to `metastore`
  - `extension_config.cmake` loads `quack` → must change to `metastore`
  - `test/sql/quack.test` → starting point for `test/sql/metastore/generic/*.test`
- Build uses cmake + vcpkg; `make` + `make test` are the primary commands
- SQLLogicTest format: see `test/sql/quack.test` for structure (`require <ext>`, `statement error`, `query I`)
- Extension registration pattern: `DUCKDB_CPP_EXTENSION_ENTRY(name, loader)` + `loader.RegisterFunction(...)`
- `ExtensionLoader` used for function/storage registration (not direct db reference)

### Architecture Decisions (from plan + METASTORE_EXTENSION_PLAN.md)
- Hybrid: C++ DuckDB shell + Rust connector core behind C ABI
- Strict provider order: Generic Framework → HMS → Glue → Dataproc (NON-BYPASSABLE)
- Single auth chokepoint - NEVER in provider adapters
- Repo layout target:
  - `src/metastore_extension.cpp` - entrypoint
  - `src/metastore_functions.cpp` - table functions registration
  - `src/storage/metastore_storage_ext.cpp` - ATTACH integration
  - `src/auth/metastore_secret_bridge.cpp` - SecretManager lookup
  - `src/providers/bridge_ffi.cpp` - C++ <-> Rust boundary
  - `rust/crates/metastore-{core,hive,glue,dataproc,ffi}/`
  - `test/sql/metastore/{generic,hive,glue,dataproc}/*.test`

### Wave 1 Task Dependencies (actual)
- T1 (extension shell) → blocks T5 (SQL API needs register hooks), T7, T8
- T2 (domain model) → blocks T3 (FFI uses model types), T7, T9, T12, T15
- T3 (connector interface/ABI) → blocked by T2 → blocks T9, T12, T15
- T4 (auth chokepoint) → blocked by T1 → blocks T9, T12, T15
- T5 (SQL API) → blocked by T1+T2 → blocks T8, T17
- T6 (diagnostics) → blocked by T1 → blocks T9, T12, T15, T17

### Guardrails Enforced
- No provider-specific logic in T1/T5/T6
- No provider-specific auth in provider adapters (all via T4 chokepoint)
- No Kerberos/SASL in v1 baseline
- No advanced pushdown in T7 (safe baseline only)

## [2026-02-21] T3 - Connector Interface & FFI Contract

### Files Created
- `src/include/metastore_connector.hpp` — IMetastoreConnector pure virtual class + MetastoreResult<T> + MetastoreError
- `src/include/metastore_ffi.h` — C ABI header with opaque handle, error envelope, all connector operations
- `src/providers/bridge_ffi.cpp` — FFIMetastoreConnector class bridging C ABI to C++ interface

### Key Design Choices
- **Result pattern**: `MetastoreResult<T>` with `MetastoreError` (code + message + detail + retryable) — consistent with plan's error envelope spec
- **C ABI naming**: `MetastoreFFIErrorCode` for C enum (avoids collision with C++ `MetastoreErrorCode` enum class when both headers included)
- **All FFI structs prefixed `MetastoreFFI*`** to avoid collision with C++ domain types
- **Error free pattern**: Every FFI call returns `MetastoreFFIError` by value; caller always calls `metastore_error_free()` (even on OK — idempotent pattern)
- **Null semantics in FFI**: Optional fields use `NULL` pointer; `predicate=""` maps to `NULL` at the bridge
- **GetTableStats** has a default implementation returning `Unsupported` — connector implementations can override

### Gotchas
- No C++ compiler available in CI environment — compilation verified by manual review only
- `MetastoreErrorCode` name collision between C header (typedef enum) and C++ header (enum class) — resolved by renaming C version to `MetastoreFFIErrorCode`
- `<functional>` header was unused — removed
- `<cstring>` header was unused in bridge — removed

## T6: Diagnostics and Error Taxonomy ✅

### Status: COMPLETE (Commit: 227452c)

**Deliverables Verified:**
1. ✅ `src/include/metastore_errors.hpp` created with:
   - `MetastoreErrorCode` enum: Ok, NotFound, PermissionDenied, Transient, InvalidConfig, Unsupported, InternalError
   - `MetastoreDiagnosticInfo` struct: provider_type, endpoint_mode, auth_strategy_class (all redacted for safe logging)
   - `MetastoreErrorTag` struct: provider, operation, retryable (bool)
   - `MetastoreException` class extending std::runtime_error with error code + tag
   - `throw_metastore_error()` helper function
   - `MetastoreErrorCodeToString()` converter

2. ✅ `src/metastore_extension.cpp` updated to:
   - Include metastore_errors.hpp header
   - Register "metastore_debug" boolean option with RegisterExtensionOption()
   - Default value: false

3. ✅ Files compile successfully (included in existing build)

### Key Design Decisions:
- Used `std::runtime_error` as base class (simpler than checking for duckdb::Exception availability)
- All diagnostic fields are string-based (not raw secret values) for safe logging
- Error taxonomy is provider-agnostic - provider-specific codes added in T9/T12/T15
- Option registration happens in LoadInternal() - ready for diagnostic wiring in T9+

### Dependencies Met:
- Blocked by T1 ✅ (working extension shell with LoadInternal available)
- Blocks T9 (HMS), T12 (Glue), T15 (Dataproc) ✅ (error taxonomy ready)

### Notes for Future Tasks:
- T9/T12/T15 will use `throw_metastore_error()` helper in their connector implementations
- Diagnostic logging wiring (when metastore_debug=true) deferred to those tasks
- Error tags should be filled consistently: provider="hms"/"glue"/"dataproc", operation=function_name, retryable based on error code

## [2026-02-21] T7 - Generic planning and pushdown baseline

### Baseline planner contract implemented
- Added `src/planner/metastore_planner.hpp` with POD planning types:
  - `MetastoreScanFilter` with optional namespace/table filters and partition predicate pairs
  - `MetastorePlannerResult` with `scan_filter`, `partition_pruning_enabled`, and diagnostic `reason`
- Added stateless `MetastorePlanner` utility with static-only methods:
  - `Plan(table, requested_namespaces, requested_tables)` for conservative namespace/table filter setup
  - `CanPrunePartitions(table)` that returns true only for explicit non-empty partition specs

### Safety rules reinforced
- Partition pruning remains disabled for unpartitioned tables (`partition_spec.columns.empty()`)
- Planner intentionally avoids provider-specific behavior and optimizer/runtime coupling
- Default behavior stays conservative: pushdown is only enabled when metadata is explicit and safe
