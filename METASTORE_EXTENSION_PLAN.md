# DuckDB Metastore Extension Plan

## Purpose

Build a DuckDB extension that can query tables registered in external metastores, with execution priority:

1. Generic metastore framework (foundation)
2. Hive Metastore (HMS)
3. AWS Glue Data Catalog
4. Google Dataproc Metastore (least important)

This plan combines:

- Proven patterns from this repository (`duckdb-delta`)
- DuckDB extension API patterns from official and community code
- Practical ecosystem tradeoffs for Rust vs C++ implementation

---

## Executive Recommendation

Use a **hybrid architecture**:

- **C++ extension shell** for DuckDB-facing integration (entrypoint, attach, table functions, secret lookup)
- **Rust connector core** for metastore clients (HMS/Glue/Dataproc adapters) behind a narrow C ABI

Why:

- Matches successful pattern in this repo: C++ extension + Rust engine over FFI
- Keeps DuckDB integration ergonomic and stable
- Leverages stronger modern SDK ecosystem in Rust (especially async and cloud auth)

Fallback option: pure C++ if team constraints require zero Rust.

---

## Best Repo Starting Point

Recommended bootstrap: start from `duckdb/extension-template`, then add hybrid scaffolding immediately.

Why this is the best starting point:

- It provides a clean DuckDB-native extension skeleton (build/load/release) without Delta-specific coupling.
- It avoids inheriting `duckdb-delta` complexity that is unrelated to metastore work.
- It still lets us reuse proven patterns from this repo (auth translation chokepoint, FFI discipline, test/CI structure).

What to carry over from this repo (pattern-level, not blind copy):

- Single auth/config normalization chokepoint pattern from `src/functions/delta_scan/delta_multi_file_list.cpp`
- Strict FFI boundary/error/ownership utility pattern from `src/include/delta_utils.hpp` and `src/delta_utils.cpp`
- SQLLogic and CI matrix style from `test/sql/cloud/**` and `.github/workflows/*.yml`

When to choose a different start:

- Fork `duckdb-delta` only if you explicitly need its Delta/kernel coupling and want to inherit its exact build graph.

---

## What We Learned From This Repo (Reusable Patterns)

## 1) Extension lifecycle and registration

- Register table/scalar functions during extension `Load`
- Register storage extension for `ATTACH ... (TYPE ...)`
- Add extension options for behavior flags/debugging

References:

- `src/delta_extension.cpp`
- `src/delta_functions.cpp`
- `src/include/delta_extension.hpp`

## 2) Cross-cloud auth normalization chokepoint

- Centralize path parsing, secret resolution, and backend option mapping in one function (`CreateBuilder`)
- Use DuckDB `SecretManager` lookup by path + secret type
- Normalize provider-specific URI/auth differences early

Reference:

- `src/functions/delta_scan/delta_multi_file_list.cpp`

## 3) FFI boundary discipline (if Rust core is used)

- Keep Rust FFI in a strict boundary layer
- Convert errors centrally (`TryUnpackResult`-style)
- Use ownership wrappers for FFI handles

References:

- `src/include/delta_kernel_ffi.hpp`
- `src/include/delta_utils.hpp`
- `src/delta_utils.cpp`

## 4) Scan integration style

- Reuse DuckDB multi-file scan patterns where possible
- Keep user API simple (`delta_scan(...)` style equivalent)
- Hide backend complexity under internal reader/list abstractions

References:

- `src/functions/delta_scan/delta_scan.cpp`
- `src/functions/delta_scan/delta_multi_file_reader.cpp`

## 5) Test and CI pattern

- SQLLogic tests encode required extension dependencies and auth modes
- CI validates local emulator + cloud paths

References:

- `test/sql/cloud/minio_local/minio_local.test`
- `test/sql/cloud/minio_local/gcs_r2.test`
- `test/sql/cloud/azure/cli_auth.test`
- `.github/workflows/LocalTesting.yml`
- `.github/workflows/CloudTesting.yml`

---

## DuckDB API Patterns To Follow

- `DUCKDB_CPP_EXTENSION_ENTRY(...)` + `Extension::Load`
- `StorageExtension::Register(config, "name", ...)` for attachable backends
- `ExtensionLoader::RegisterFunction(...)` for table functions
- `SecretManager::LookupSecret(...)` for auth discovery

External evidence/examples:

- `duckdb/duckdb` extension loader and storage extension APIs
- `duckdb/duckdb-postgres`, `duckdb/duckdb-sqlite`, `duckdb/duckdb-delta`
- `duckdb/unity_catalog` as a metastore-like attach pattern

---

## Rust vs C++ Decision (Weighted)

## Weighted score for this project

- **Hybrid (C++ shell + Rust connectors): 8.5/10 (recommended)**
- **Pure C++ end-to-end: 6.5/10**

## Rust/hybrid advantages

- Strong cloud SDK ecosystem for this exact domain
  - Rust Glue client usage is common (`aws_sdk_glue::Client::new(...)`) in data projects
  - Rust HMS ecosystem exists and is used in modern table-format projects
- Better memory safety in connector/client-heavy code
- Faster iteration on multi-cloud auth and API wrappers

## Rust/hybrid costs

- FFI boundary complexity (ABI and ownership contracts)
- Async runtime boundary decisions (Tokio inside Rust side)
- More moving parts in build/release

## Pure C++ advantages

- Single-language integration with DuckDB internals
- No FFI layer
- Debugging simplicity in DuckDB-centric workflows

## Pure C++ costs

- Heavier SDK/toolchain burden for cloud clients
- More manual memory/concurrency risk
- Slower connector iteration velocity in practice

---

## Delivery Plan

## Phase 0 - Foundation (Generic, required first)

Goal: establish reusable framework before provider-specific work.

### 0.1 Extension shape

- Create extension shell in C++:
  - entrypoint
  - `Load` registration
  - storage extension registration
  - extension options

### 0.2 User-facing SQL API (generic)

- `ATTACH '<uri-or-name>' AS <db_alias> (TYPE metastore, PROVIDER <...>, ...)`
- Table function fallback:
  - `metastore_scan('<catalog>', '<schema>', '<table>' [, options])`

### 0.3 Generic metastore model

Define internal provider-agnostic model:

- `Catalog`
- `Namespace/Database`
- `Table`
- `StorageDescriptor` (location, format, serde/input/output)
- `PartitionSpec` and partition values
- Table properties map

### 0.4 Connector interface (language-neutral contract)

Define a single adapter trait/interface:

- `ListNamespaces`
- `ListTables(namespace)`
- `GetTable(namespace, table)`
- `ListPartitions(namespace, table, predicate?)`
- `GetTableStats` (optional)

If Rust core:

- expose C ABI with opaque handles and explicit free functions
- use one error envelope format (code + message + detail)

### 0.5 Secret and auth normalization layer (critical)

Implement one chokepoint (equivalent to `CreateBuilder`):

- infer provider/type from URI/attach config
- resolve secret using DuckDB `SecretManager`
- map to connector config struct
- validate required fields
- return actionable errors

### 0.6 Pushdown + planning contract

- Start with safe baseline:
  - namespace/table-level pruning
  - partition pruning only when metadata is explicit and reliable
- Avoid premature heavy pushdown assumptions

### 0.7 Generic observability and diagnostics

- debug mode option (list resolved provider, endpoint mode, auth strategy class, redacted)
- structured error tags (`provider`, `operation`, `retryable`)

### 0.8 Generic tests and CI (must exist before provider phases complete)

- unit tests for adapter contracts and secret mapping
- SQLLogic end-to-end tests with fixtures
- CI lanes:
  - no-credential local tests
  - credential-injected integration tests (gated)

Deliverables:

- working extension skeleton
- provider-agnostic connector contract
- full secret mapping framework
- base SQL API and tests

---

## Phase 1 - Hive Metastore (highest provider priority)

Goal: deliver robust HMS support first because it is the most general metastore interoperability layer.

### 1.1 HMS connector implementation

- Implement `hive` provider adapter over Thrift HMS APIs:
  - list databases
  - list tables
  - get table metadata
  - list/get partitions
- Support metastore URI variants and transport config

### 1.2 Authentication and connection modes

- Initial support:
  - unauthenticated/internal network HMS
  - username/password if applicable
- staged support:
  - TLS
  - Kerberos/SASL (optional milestone, behind feature flag)

### 1.3 Metadata-to-DuckDB mapping

- Parse Hive storage descriptors and map to DuckDB scans:
  - Parquet/ORC first
  - serde edge cases flagged clearly
- Partition metadata conversion and predicate pruning

### 1.4 Reliability features

- connection pooling
- retries with backoff for transient failures
- bounded metadata pagination

### 1.5 HMS test matrix

- local HMS (dockerized) integration tests
- partition-heavy table scenarios
- malformed metadata cases

Deliverables:

- production-ready HMS read path
- attach + browse + query through HMS-backed tables

---

## Phase 2 - AWS Glue Data Catalog (second priority)

Goal: add Glue as cloud-native catalog with strong auth behavior.

### 2.1 Glue connector adapter

- Implement Glue operations:
  - list databases
  - list tables
  - get table
  - get partitions (paged)

### 2.2 Auth integration through DuckDB secrets

Map DuckDB secret sources to Glue client config:

- static keys
- credential chain/profile/role
- session token
- region and optional endpoint override

### 2.3 Lake location handoff

- Glue table location -> downstream file scan (S3 etc.)
- reuse the repository's cloud secret normalization pattern for storage path access

### 2.4 Glue-specific behaviors

- account/region scoping
- throttling-aware retries
- eventual consistency handling in listing flows

### 2.5 Glue test matrix

- localstack-style tests where possible
- real AWS integration tests (gated CI) for IAM chain modes

Deliverables:

- Glue provider support with robust auth and pagination
- validated interoperability with S3-backed tables

---

## Phase 3 - Google Dataproc Metastore (least important)

Goal: provide functional support after HMS and Glue are stable.

### 3.1 Dataproc adapter scope

- Implement minimal viable operations first:
  - list databases/namespaces
  - list tables
  - get table
- partition APIs and advanced metadata as follow-up

### 3.2 Auth and endpoint handling

- service account credentials / ADC style config
- region/service endpoint mapping

### 3.3 Compatibility strategy

- focus on read path for common Hive-compatible metadata flows
- maintain explicit unsupported-feature errors for gaps

### 3.4 Dataproc tests

- mocked API contract tests
- gated cloud integration tests (lower frequency lane)

Deliverables:

- baseline Dataproc provider support
- documented limitations and roadmap

---

## Suggested Repository Layout

```text
src/
  metastore_extension.cpp            # C++ extension entrypoint/load
  metastore_functions.cpp            # table functions registration
  storage/metastore_storage_ext.cpp  # ATTACH integration
  auth/metastore_secret_bridge.cpp   # SecretManager lookup + mapping
  providers/
    bridge_ffi.cpp                   # C++ <-> Rust boundary (if hybrid)
rust/
  crates/
    metastore-core/                  # provider-agnostic domain model
    metastore-hive/
    metastore-glue/
    metastore-dataproc/
    metastore-ffi/                   # C ABI surface
test/sql/metastore/
  generic/*.test
  hive/*.test
  glue/*.test
  dataproc/*.test
```

---

## Milestones and Exit Criteria

## M0: Framework complete

- generic API works
- attach + table function available
- secret normalization tested

## M1: Hive complete

- list/query partitioned tables via HMS
- stable retries/timeouts
- local integration CI green

## M2: Glue complete

- Glue metadata queries + S3 location handoff
- credential chain modes validated
- gated cloud tests green

## M3: Dataproc baseline complete

- core read metadata operations
- documented feature gaps

---

## Risks and Mitigations

## Risk: FFI complexity in hybrid model

Mitigation:

- keep C ABI tiny and stable
- strict ownership/free contracts
- exhaustive boundary tests

## Risk: auth mode explosion

Mitigation:

- single secret translation chokepoint
- provider-specific mappers with explicit validation

## Risk: metadata inconsistency and scale

Mitigation:

- paginated fetches
- deterministic caching policy
- retry/backoff with clear retryable classification

## Risk: protocol-specific HMS quirks

Mitigation:

- start with common Hive versions and transports
- expand compatibility matrix incrementally

---

## Implementation Order (do not reorder)

1. Generic framework and contracts
2. Hive provider
3. Glue provider
4. Dataproc provider

---

## References (Key Evidence)

Repo-local:

- `src/delta_extension.cpp`
- `src/delta_functions.cpp`
- `src/functions/delta_scan/delta_multi_file_list.cpp`
- `src/functions/delta_scan/delta_scan.cpp`
- `src/include/delta_kernel_ffi.hpp`
- `src/include/delta_utils.hpp`
- `src/delta_utils.cpp`
- `test/sql/cloud/minio_local/minio_local.test`
- `test/sql/cloud/minio_local/gcs_r2.test`
- `test/sql/cloud/azure/cli_auth.test`

External:

- DuckDB storage extension API (`duckdb/duckdb`): `storage_extension.hpp`
- DuckDB extension entry macro (`DUCKDB_CPP_EXTENSION_ENTRY`)
- DuckDB examples: `duckdb-postgres`, `duckdb-sqlite`, `duckdb-delta`
- Apache Hive thrift IDL: `standalone-metastore/.../hive_metastore.thrift`
- Rust Glue client docs: `docs.rs/aws-sdk-glue`
- Dataproc Metastore reference: Google Cloud docs (REST/RPC/client references)
