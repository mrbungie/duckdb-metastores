# Metastore Extension Execution Plan

## TL;DR

> **Quick Summary**: Convert the strategy in `METASTORE_EXTENSION_PLAN.md` into a delivery-ready execution plan with strict provider ordering, concrete task gates, and agent-executable verification.
>
> **Deliverables**:
> - One implementation-ready task graph with parallel waves and explicit dependencies
> - Provider rollout plan: Generic framework -> HMS -> Glue -> Dataproc
> - Command-verifiable acceptance criteria and QA scenarios per task
>
> **Estimated Effort**: Large
> **Parallel Execution**: YES - 4 waves + final verification wave
> **Critical Path**: T1 -> T3 -> T4 -> T9 -> T12 -> T15 -> T17 -> T18

---

## Context

### Original Request
Make an actual plan from `METASTORE_EXTENSION_PLAN.md`.

### Interview Summary
**Key Discussions**:
- User requested immediate plan generation from the provided source document.
- Existing source already defines architecture direction and provider priority, but not execution-grade task cards.

**Research Findings**:
- Source includes reusable local patterns for extension registration, auth normalization chokepoint, FFI discipline, and cloud-style test/CI patterns.
- Source milestones are mostly qualitative and needed conversion to command-verifiable criteria.

### Metis Review
**Identified Gaps (addressed)**:
- Missing hard scope boundaries per phase -> resolved with explicit IN/OUT guardrails.
- Risk of provider parity scope creep -> resolved with per-provider v1 capability locks.
- Weak acceptance criteria wording -> resolved with command-based checks and QA scenarios.
- FFI/auth drift risk -> resolved with mandatory chokepoint + ABI freeze gate.

---

## Work Objectives

### Core Objective
Deliver a single execution-ready plan that an implementation agent can run without interpretation drift, while preserving the required provider order and hybrid architecture baseline.

### Concrete Deliverables
- A full task graph with dependencies, parallel waves, and recommended agent profiles.
- Explicit scope controls and anti-scope-creep guardrails.
- Agent-executable acceptance and QA verification strategy for every task.

### Definition of Done
- [ ] `.sisyphus/plans/metastore-extension-execution-plan.md` exists and is complete.
- [ ] Plan includes all phases (Generic, HMS, Glue, Dataproc) in the required order.
- [ ] Every task includes concrete acceptance criteria and at least one happy + one negative QA scenario.

### Must Have
- Provider order is non-bypassable: Generic -> HMS -> Glue -> Dataproc.
- Single secret/auth normalization chokepoint.
- Strict C++/Rust FFI boundary discipline for hybrid architecture.
- Milestone gates M0-M3 with objective checks.

### Must NOT Have (Guardrails)
- No provider reordering.
- No parity scope inflation (Glue/Dataproc forced to HMS depth in v1).
- No provider-specific auth logic outside normalization chokepoint.
- No manual-only acceptance criteria.

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** - all acceptance criteria are agent-executed.

### Test Decision
- **Infrastructure exists**: YES
- **Automated tests**: YES (tests-after)
- **Framework**: SQLLogic + C++ unit/integration test stack (plus provider integration harnesses)
- **TDD Mode**: Not mandatory; task-level verification occurs after implementation slices.

### QA Policy
Evidence path convention: `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`

- **Frontend/UI**: Not applicable for this plan.
- **TUI/CLI**: Use `interactive_bash` for long-running test harnesses when needed.
- **API/Backend**: Use Bash (`curl`, CLI clients, DuckDB shell commands) with explicit assertions.
- **Library/Module**: Use Bash build/test commands and SQLLogic runs.

---

## Execution Strategy

### Parallel Execution Waves

Wave 1 (Foundation - start immediately):
- T1 Extension shell and lifecycle registration
- T2 Provider-agnostic metastore model
- T3 Connector interface and C ABI contract
- T4 Secret/auth normalization chokepoint
- T5 SQL API surface (`ATTACH` + `metastore_scan`)
- T6 Diagnostics and error taxonomy

Wave 2 (After Wave 1):
- T7 Generic planning and pushdown baseline
- T8 Generic tests + CI lanes
- T9 HMS connector core operations
- T10 HMS metadata mapping + reliability controls
- T11 HMS integration matrix

Wave 3 (After Wave 2):
- T12 Glue connector + auth + throttling
- T13 Glue storage-location handoff
- T14 Glue test matrix
- T15 Dataproc baseline adapter
- T16 Dataproc tests + limitations contract

Wave 4 (After Wave 3):
- T17 Cross-provider conformance suite
- T18 Milestone gate validation (M0-M3)
- T19 Release packaging + CI hardening checks

Wave FINAL (After all tasks):
- F1 Plan compliance audit
- F2 Code quality review
- F3 End-to-end QA execution
- F4 Scope fidelity check

Critical Path: T1 -> T3 -> T4 -> T9 -> T12 -> T15 -> T17 -> T18
Parallel Speedup: ~60-70% vs strict sequential execution
Max Concurrent: 6 (Wave 1)

### Dependency Matrix

- **T1**: blocked by none -> blocks T5, T7, T8
- **T2**: blocked by none -> blocks T3, T7, T9, T12, T15
- **T3**: blocked by T2 -> blocks T9, T12, T15
- **T4**: blocked by T1 -> blocks T9, T12, T15
- **T5**: blocked by T1, T2 -> blocks T8, T17
- **T6**: blocked by T1 -> blocks T9, T12, T15, T17
- **T7**: blocked by T1, T2, T5 -> blocks T10, T13, T17
- **T8**: blocked by T1, T5 -> blocks T11, T14, T16, T18
- **T9**: blocked by T2, T3, T4, T6 -> blocks T10, T11
- **T10**: blocked by T7, T9 -> blocks T11, T17
- **T11**: blocked by T8, T9, T10 -> blocks T18
- **T12**: blocked by T2, T3, T4, T6 -> blocks T13, T14
- **T13**: blocked by T7, T12 -> blocks T14, T17
- **T14**: blocked by T8, T12, T13 -> blocks T18
- **T15**: blocked by T2, T3, T4, T6 -> blocks T16
- **T16**: blocked by T8, T15 -> blocks T18
- **T17**: blocked by T5, T6, T7, T10, T13 -> blocks T18
- **T18**: blocked by T11, T14, T16, T17 -> blocks T19, FINAL
- **T19**: blocked by T18 -> blocks FINAL
- **F1-F4**: blocked by T19 and all implementation tasks

### Agent Dispatch Summary

- **Wave 1 (6 tasks)**: T1 `quick`, T2 `unspecified-high`, T3 `unspecified-high`, T4 `quick`, T5 `quick`, T6 `quick`
- **Wave 2 (5 tasks)**: T7 `deep`, T8 `quick`, T9 `unspecified-high`, T10 `deep`, T11 `unspecified-high`
- **Wave 3 (5 tasks)**: T12 `unspecified-high`, T13 `deep`, T14 `unspecified-high`, T15 `unspecified-high`, T16 `quick`
- **Wave 4 (3 tasks)**: T17 `deep`, T18 `deep`, T19 `quick`
- **Final (4 tasks)**: F1 `oracle`, F2 `unspecified-high`, F3 `unspecified-high`, F4 `deep`

---

## TODOs

- **Phase 0 - Generic Foundation**
- **Scope IN**: Extension shell, generic contracts, auth normalization, SQL API, baseline diagnostics.
- **Scope OUT**: Provider-specific API calls, Kerberos/SASL deep support, aggressive pushdown optimizations.

- [x] 1. Build extension shell and lifecycle registration

  **What to do**:
  - Create metastore extension entrypoint and `Load` wiring for extension options and registration hooks.
  - Register storage extension skeleton for `ATTACH ... (TYPE metastore)`.

  **Must NOT do**:
  - Do not add provider-specific logic.

  **Recommended Agent Profile**:
  - **Category**: `quick` - straightforward extension bootstrap in limited files.
  - **Skills**: `git-master` (commit hygiene).

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with T2-T6)
  - **Blocks**: T5, T7, T8
  - **Blocked By**: None

  **References**:
  - `src/quack_extension.cpp` - extension entrypoint and load/registration baseline to mirror.
  - `src/include/quack_extension.hpp` - extension class/header structure baseline.

  **Acceptance Criteria**:
  - [x] `grep -R "DUCKDB_CPP_EXTENSION_ENTRY" src` returns metastore entrypoint.
  - [x] Build command for extension target succeeds.

  **QA Scenarios**:
  ```text
  Scenario: Extension loads successfully
    Tool: Bash
    Steps: Build extension; load extension in DuckDB shell; run pragma to list loaded extensions.
    Expected Result: Metastore extension appears in loaded extensions list.
    Evidence: .sisyphus/evidence/task-1-extension-load.txt

  Scenario: Invalid load path fails clearly
    Tool: Bash
    Steps: Attempt LOAD from wrong path.
    Expected Result: Non-zero exit and actionable error text.
    Evidence: .sisyphus/evidence/task-1-invalid-load-error.txt
  ```

  **Commit**: YES
  - Message: `feat(metastore): scaffold extension shell`

- [x] 2. Define provider-agnostic metastore domain model

  **What to do**:
  - Add internal model types for Catalog, Namespace, Table, StorageDescriptor, PartitionSpec, and table properties.
  - Keep contracts neutral across HMS/Glue/Dataproc.

  **Must NOT do**:
  - Do not encode provider-specific fields in base model.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` - core abstraction affects all downstream tasks.
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T3, T7, T9, T12, T15
  - **Blocked By**: None

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:191` - required generic model entities.
  - `METASTORE_EXTENSION_PLAN.md:191` - canonical neutral entity set for domain model.

  **Acceptance Criteria**:
  - [x] Model compiles with no provider imports.
  - [x] Unit test validates serialization/deserialization of model objects.

  **QA Scenarios**:
  ```text
  Scenario: Model handles canonical table metadata
    Tool: Bash
    Steps: Run unit tests for domain model using fixture table metadata.
    Expected Result: Tests pass and required fields persist correctly.
    Evidence: .sisyphus/evidence/task-2-model-happy.txt

  Scenario: Missing mandatory metadata rejected
    Tool: Bash
    Steps: Run negative unit case with missing location/format.
    Expected Result: Validation error with field-specific message.
    Evidence: .sisyphus/evidence/task-2-model-error.txt
  ```

  **Commit**: YES
  - Message: `feat(metastore): add provider-agnostic metadata model`

- [x] 3. Implement connector interface and C ABI boundary contract

  **What to do**:
  - Define neutral connector interface (`ListNamespaces`, `ListTables`, `GetTable`, `ListPartitions`, optional stats).
  - If hybrid mode, expose stable C ABI with opaque handles and explicit free functions.

  **Must NOT do**:
  - Do not leak Rust runtime or provider types into DuckDB-facing C++ code.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` - ABI and ownership contracts are critical.
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T9, T12, T15
  - **Blocked By**: T2

  **References**:
  - `src/include/quack_extension.hpp` - local include/style conventions for public extension headers.
  - `METASTORE_EXTENSION_PLAN.md:83` - FFI boundary discipline and ownership guidance.

  **Acceptance Criteria**:
  - [x] ABI symbols compile and link from C++ side.
  - [x] Ownership/free contract test passes under sanitizer checks.

  **QA Scenarios**:
  ```text
  Scenario: FFI round-trip returns table metadata
    Tool: Bash
    Steps: Run ABI smoke test invoking connector methods through C ABI.
    Expected Result: Valid metadata response and zero leaks.
    Evidence: .sisyphus/evidence/task-3-ffi-happy.txt

  Scenario: FFI returns structured error envelope
    Tool: Bash
    Steps: Trigger connector error (unknown namespace).
    Expected Result: Error includes code/message/detail fields.
    Evidence: .sisyphus/evidence/task-3-ffi-error.txt
  ```

  **Commit**: YES
  - Message: `feat(metastore): add connector interface and ffi contract`

- [x] 4. Build single secret/auth normalization chokepoint

  **What to do**:
  - Implement one mapping layer from attach/table options to provider connector configs.
  - Resolve credentials via DuckDB `SecretManager` and validate required fields.

  **Must NOT do**:
  - Do not perform secret mapping inside provider adapters.

  **Recommended Agent Profile**:
  - **Category**: `quick` - focused integration utility with high leverage.
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T9, T12, T15
  - **Blocked By**: T1

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:217` - single auth normalization chokepoint contract.
  - `METASTORE_EXTENSION_PLAN.md:217` - auth chokepoint requirements.

  **Acceptance Criteria**:
  - [x] Unit tests cover provider inference + secret resolution + field validation.
  - [x] Error messages identify provider, operation, and missing config field.

  **QA Scenarios**:
  ```text
  Scenario: Valid secret resolves to connector config
    Tool: Bash
    Steps: Run auth mapping test with configured secret.
    Expected Result: Connector config contains expected endpoint/region/credential mode.
    Evidence: .sisyphus/evidence/task-4-auth-happy.txt

  Scenario: Missing secret fails with actionable message
    Tool: Bash
    Steps: Execute attach call without required secret.
    Expected Result: Error indicates missing secret type and key.
    Evidence: .sisyphus/evidence/task-4-auth-error.txt
  ```

  **Commit**: YES
  - Message: `feat(metastore): add centralized auth normalization bridge`

- [x] 5. Implement SQL API surface (`ATTACH` + `metastore_scan`)

  **What to do**:
  - Register attachable metastore backend and table function fallback for direct scans.
  - Ensure provider and options parsing are consistent between attach and scan entrypoints.

  **Must NOT do**:
  - Do not embed provider-specific API behavior in function registration layer.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T8, T17
  - **Blocked By**: T1, T2

  **References**:
  - `src/quack_extension.cpp` - current extension registration style to extend.
  - `METASTORE_EXTENSION_PLAN.md:185` - required SQL API forms.

  **Acceptance Criteria**:
  - [x] `ATTACH ... (TYPE metastore, PROVIDER ...)` parses and binds.
  - [x] `metastore_scan(catalog, schema, table, options)` binds and returns relation schema.

  **QA Scenarios**:
  ```text
  Scenario: Attach and query metadata path works
    Tool: Bash
    Steps: Run DuckDB script with valid metastore attach and metadata query.
    Expected Result: Query returns expected table listing.
    Evidence: .sisyphus/evidence/task-5-sqlapi-happy.txt

  Scenario: Invalid provider option is rejected
    Tool: Bash
    Steps: Execute attach with unknown provider value.
    Expected Result: Clear validation error with accepted provider list.
    Evidence: .sisyphus/evidence/task-5-sqlapi-error.txt
  ```

  **Commit**: YES
  - Message: `feat(metastore): add attach and metastore_scan sql surface`

- [x] 6. Add diagnostics and structured error taxonomy

  **What to do**:
  - Add debug option exposing redacted provider/auth strategy metadata.
  - Standardize error tags: `provider`, `operation`, `retryable`.

  **Must NOT do**:
  - Do not emit raw secrets or tokens in logs/errors.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T9, T12, T15, T17
  - **Blocked By**: T1

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:234` - required structured diagnostics and tag conventions.
  - `METASTORE_EXTENSION_PLAN.md:234` - diagnostics objectives.

  **Acceptance Criteria**:
  - [x] Error envelope includes required tags for all connector failures.
  - [x] Debug mode output is redacted and deterministic.

  **QA Scenarios**:
  ```text
  Scenario: Debug mode surfaces redacted diagnostics
    Tool: Bash
    Steps: Enable debug option and run a connector call.
    Expected Result: Provider/operation appear; sensitive values redacted.
    Evidence: .sisyphus/evidence/task-6-diag-happy.txt

  Scenario: Failure path includes retryable classification
    Tool: Bash
    Steps: Inject transient connector failure in test harness.
    Expected Result: Error tag `retryable=true` set correctly.
    Evidence: .sisyphus/evidence/task-6-diag-error.txt
  ```

  **Commit**: YES
  - Message: `feat(metastore): add structured diagnostics and error tags`

- **Phase 1 - Hive Metastore (Priority 1 Provider)**
- **Scope IN**: Core HMS metadata operations, partition mapping, retries/backoff, local integration coverage.
- **Scope OUT**: Mandatory Kerberos/SASL completion, non-Hive providers.

- [x] 7. Implement generic planning and pushdown baseline

  **What to do**:
  - Implement namespace/table pruning baseline and safe partition pruning rules.
  - Ensure planner contract works across all providers before provider-specific adapters.

  **Must NOT do**:
  - Do not assume advanced predicate pushdown semantics without explicit metadata guarantees.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with T8-T11)
  - **Blocks**: T10, T13, T17
  - **Blocked By**: T1, T2, T5

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:227` - scan planning and pushdown baseline contract.
  - `METASTORE_EXTENSION_PLAN.md:227` - pushdown contract constraints.

  **Acceptance Criteria**:
  - [ ] Planner tests show namespace/table pruning correctness.
  - [ ] Partition pruning only applies when metadata has explicit partition schema.

  **QA Scenarios**:
  ```text
  Scenario: Namespace/table pruning reduces scanned objects
    Tool: Bash
    Steps: Run benchmark fixture with and without pruning predicates.
    Expected Result: Pruned run touches fewer metadata entities.
    Evidence: .sisyphus/evidence/task-7-pushdown-happy.txt

  Scenario: Unsafe partition metadata disables partition pruning
    Tool: Bash
    Steps: Use malformed partition metadata fixture.
    Expected Result: Planner logs fallback and avoids incorrect filter pushdown.
    Evidence: .sisyphus/evidence/task-7-pushdown-error.txt
  ```

  **Commit**: YES
  - Message: `feat(metastore): add safe planning and pruning baseline`

- [x] 8. Establish generic tests and CI lanes

  **What to do**:
  - Add unit and SQLLogic suites for generic contracts and auth mapping.
  - Add CI lanes for no-credential local tests and credential-gated integration runs.

  **Must NOT do**:
  - Do not require cloud credentials in default CI lane.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: T11, T14, T16, T18
  - **Blocked By**: T1, T5

  **References**:
  - `test/sql/quack.test` - local SQLLogic structure and assertion style baseline.
  - `.github/workflows/ExtensionTemplate.yml` - base local CI/test lane pattern.

  **Acceptance Criteria**:
  - [ ] Local CI lane passes without secrets.
  - [ ] Gated lane skips gracefully without credentials and runs when provided.

  **QA Scenarios**:
  ```text
  Scenario: Local CI lane passes in clean env
    Tool: Bash
    Steps: Run local test workflow command in environment without cloud secrets.
    Expected Result: Generic suites pass; cloud-gated suites skipped with explicit reason.
    Evidence: .sisyphus/evidence/task-8-ci-happy.txt

  Scenario: Misconfigured gated lane fails with actionable output
    Tool: Bash
    Steps: Run gated lane with partial credentials.
    Expected Result: Fail-fast on missing variables, clear remediation message.
    Evidence: .sisyphus/evidence/task-8-ci-error.txt
  ```

  **Commit**: YES
  - Message: `test(ci): add generic metastore test lanes`

- [x] 9. Implement HMS connector core operations

  **What to do**:
  - Implement HMS adapter calls for list databases/tables, get table metadata, list partitions.
  - Support baseline transport configuration and endpoint URI variants.

  **Must NOT do**:
  - Do not add Glue/Dataproc logic in HMS adapter.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: T10, T11
  - **Blocked By**: T2, T3, T4, T6

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:260` - HMS operation scope.
  - `METASTORE_EXTENSION_PLAN.md:498` - Hive metastore thrift reference.

  **Acceptance Criteria**:
  - [ ] Adapter contract tests pass for list/get operations and partition listing.
  - [ ] Endpoint configuration parsing supports expected URI forms.

  **QA Scenarios**:
  ```text
  Scenario: HMS adapter returns table metadata
    Tool: Bash
    Steps: Run integration fixture against local HMS container.
    Expected Result: Expected databases/tables returned with correct schema fields.
    Evidence: .sisyphus/evidence/task-9-hms-happy.txt

  Scenario: Invalid HMS endpoint fails gracefully
    Tool: Bash
    Steps: Configure unreachable HMS URI and invoke list tables.
    Expected Result: Structured connection error with retryability tag.
    Evidence: .sisyphus/evidence/task-9-hms-error.txt
  ```

  **Commit**: YES
  - Message: `feat(hms): add core hive metastore adapter operations`

- [x] 10. Add HMS metadata mapping and reliability controls

  **What to do**:
  - Map Hive storage descriptors to DuckDB scan paths (Parquet/ORC first).
  - Add retries with backoff, bounded pagination, and connection pooling hooks.

  **Must NOT do**:
  - Do not silently coerce unsupported serde edge cases; return explicit errors.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: T11, T17
  - **Blocked By**: T7, T9

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:278` - descriptor mapping expectations.
  - `METASTORE_EXTENSION_PLAN.md:285` - reliability feature list.

  **Acceptance Criteria**:
  - [ ] Mapping tests pass for Parquet and ORC fixtures.
  - [ ] Retry policy test verifies bounded retries and exponential backoff.

  **QA Scenarios**:
  ```text
  Scenario: Partitioned HMS table maps and prunes correctly
    Tool: Bash
    Steps: Query partitioned fixture with predicate.
    Expected Result: Correct results and reduced partition scan count.
    Evidence: .sisyphus/evidence/task-10-hms-map-happy.txt

  Scenario: Unsupported serde produces explicit unsupported error
    Tool: Bash
    Steps: Query fixture with unsupported serde format.
    Expected Result: Deterministic unsupported-feature error code/message.
    Evidence: .sisyphus/evidence/task-10-hms-map-error.txt
  ```

  **Commit**: YES
  - Message: `feat(hms): add metadata mapping and retry controls`

- [ ] 11. Implement HMS integration matrix

  **What to do**:
  - Add dockerized HMS integration tests covering partition-heavy and malformed metadata scenarios.
  - Wire matrix into CI with clear pass/fail gates for M1.

  **Must NOT do**:
  - Do not block local developer loop on cloud-only dependencies.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: T18
  - **Blocked By**: T8, T9, T10

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:291` - required HMS test matrix.
  - `.github/workflows/ExtensionTemplate.yml` - local integration workflow pattern.

  **Acceptance Criteria**:
  - [ ] HMS matrix command passes in local CI lane.
  - [ ] Malformed metadata case asserts explicit error behavior.

  **QA Scenarios**:
  ```text
  Scenario: HMS matrix passes with local containers
    Tool: Bash
    Steps: Run HMS integration suite in containerized env.
    Expected Result: All HMS cases pass with stable runtime.
    Evidence: .sisyphus/evidence/task-11-hms-ci-happy.txt

  Scenario: Corrupted metadata fixture fails predictably
    Tool: Bash
    Steps: Run malformed fixture test.
    Expected Result: Failure is deterministic and classified non-retryable.
    Evidence: .sisyphus/evidence/task-11-hms-ci-error.txt
  ```

  **Commit**: YES
  - Message: `test(hms): add local hive integration matrix`

- **Phase 2 - AWS Glue Data Catalog (Priority 2 Provider)**
- **Scope IN**: Glue list/get/partition operations, credential mapping, pagination, throttling, S3 handoff.
- **Scope OUT**: Cross-account feature expansion beyond stated attach/secret model.

- [ ] 12. Implement Glue connector with auth and throttling controls

  **What to do**:
  - Implement Glue adapter operations and paged traversal.
  - Map DuckDB secrets to Glue client auth modes (keys/profile/role/session token/region).
  - Add throttling-aware retry behavior.

  **Must NOT do**:
  - Do not bypass central secret normalization logic.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: T13, T14
  - **Blocked By**: T2, T3, T4, T6

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:308` - Glue adapter scope.
  - `METASTORE_EXTENSION_PLAN.md:317` - auth mapping requirements.

  **Acceptance Criteria**:
  - [ ] Glue adapter contract tests pass for list/get/partition operations.
  - [ ] Auth mode tests pass across static, chain/profile/role, and session-token flows.

  **QA Scenarios**:
  ```text
  Scenario: Glue list/get flow works with valid credentials
    Tool: Bash
    Steps: Run Glue integration harness in enabled environment.
    Expected Result: Databases/tables listed and selected table metadata returned.
    Evidence: .sisyphus/evidence/task-12-glue-happy.txt

  Scenario: Throttling path retries then surfaces bounded failure
    Tool: Bash
    Steps: Simulate Glue throttling response in test harness.
    Expected Result: Retries obey policy and final error includes retry metadata.
    Evidence: .sisyphus/evidence/task-12-glue-error.txt
  ```

  **Commit**: YES
  - Message: `feat(glue): add adapter auth mapping and retry controls`

- [ ] 13. Implement Glue storage-location handoff

  **What to do**:
  - Map Glue table storage locations to downstream object-store scan resolution.
  - Reuse generic planning/pushdown hooks for location-driven pruning.

  **Must NOT do**:
  - Do not duplicate storage secret handling logic inside Glue adapter.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: T14, T17
  - **Blocked By**: T7, T12

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:325` - Glue location handoff requirement.
  - `METASTORE_EXTENSION_PLAN.md:325` - Glue location handoff and normalization requirements.

  **Acceptance Criteria**:
  - [ ] Glue location mapping test routes S3 paths into expected scan planner path.
  - [ ] Invalid location URI returns structured validation error.

  **QA Scenarios**:
  ```text
  Scenario: Glue table location resolves to S3 scan path
    Tool: Bash
    Steps: Resolve fixture Glue table and execute metadata-backed query.
    Expected Result: Query planner uses resolved S3 location and returns expected rows.
    Evidence: .sisyphus/evidence/task-13-glue-location-happy.txt

  Scenario: Unsupported location scheme rejected
    Tool: Bash
    Steps: Use fixture with unsupported storage scheme.
    Expected Result: Explicit unsupported storage scheme error.
    Evidence: .sisyphus/evidence/task-13-glue-location-error.txt
  ```

  **Commit**: YES
  - Message: `feat(glue): add storage location handoff into scan planner`

- [ ] 14. Add Glue test matrix (local and gated cloud)

  **What to do**:
  - Add localstack-compatible tests where possible.
  - Add gated real AWS integration lane validating IAM chain modes.

  **Must NOT do**:
  - Do not force real AWS credentials for local developer workflow.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: T18
  - **Blocked By**: T8, T12, T13

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:336` - Glue matrix requirements.
  - `.github/workflows/MainDistributionPipeline.yml` - gated distribution/cloud-style pipeline pattern.

  **Acceptance Criteria**:
  - [ ] Localstack lane passes with deterministic fixtures.
  - [ ] Gated AWS lane validates credential chain modes when secrets are set.

  **QA Scenarios**:
  ```text
  Scenario: Localstack Glue matrix passes
    Tool: Bash
    Steps: Run Glue local integration suite.
    Expected Result: All local Glue tests pass.
    Evidence: .sisyphus/evidence/task-14-glue-ci-happy.txt

  Scenario: Missing AWS role var fails with clear CI message
    Tool: Bash
    Steps: Execute gated job without required role variable.
    Expected Result: Controlled fail with required var list.
    Evidence: .sisyphus/evidence/task-14-glue-ci-error.txt
  ```

  **Commit**: YES
  - Message: `test(glue): add local and gated cloud matrix`

- **Phase 3 - Google Dataproc Metastore (Priority 3 Provider)**
- **Scope IN**: Baseline list/get operations, auth/endpoint mapping, explicit unsupported-feature handling.
- **Scope OUT**: Full partition API parity, advanced metadata parity with HMS.

- [ ] 15. Implement Dataproc baseline adapter

  **What to do**:
  - Add Dataproc adapter for list namespaces, list tables, and get table.
  - Support service-account/ADC credentials and regional endpoint mapping.

  **Must NOT do**:
  - Do not claim partition API support in v1 baseline.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: T16
  - **Blocked By**: T2, T3, T4, T6

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:352` - Dataproc baseline scope.
  - `METASTORE_EXTENSION_PLAN.md:362` - auth/endpoint handling requirements.

  **Acceptance Criteria**:
  - [ ] Contract tests pass for baseline list/get operations.
  - [ ] Endpoint mapping tests validate region/service endpoint resolution.

  **QA Scenarios**:
  ```text
  Scenario: Dataproc baseline metadata lookup succeeds
    Tool: Bash
    Steps: Run Dataproc baseline fixture with valid ADC credentials.
    Expected Result: Namespace/table listing and get-table responses are returned.
    Evidence: .sisyphus/evidence/task-15-dataproc-happy.txt

  Scenario: Invalid region mapping rejected
    Tool: Bash
    Steps: Configure unsupported region endpoint and run list call.
    Expected Result: Validation failure with endpoint mapping hint.
    Evidence: .sisyphus/evidence/task-15-dataproc-error.txt
  ```

  **Commit**: YES
  - Message: `feat(dataproc): add baseline adapter and auth mapping`

- [ ] 16. Add Dataproc tests and unsupported-feature contract

  **What to do**:
  - Add mocked API contract tests and lower-frequency gated integration lane.
  - Define explicit unsupported-feature errors for partition/advanced metadata gaps.

  **Must NOT do**:
  - Do not silently no-op unsupported requests.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: T18
  - **Blocked By**: T8, T15

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:370` - Dataproc test scope.
  - `METASTORE_EXTENSION_PLAN.md:366` - compatibility and unsupported-feature strategy.

  **Acceptance Criteria**:
  - [ ] Mock contract suite passes and verifies unsupported feature error contracts.
  - [ ] Gated integration lane runs on schedule/manual trigger without affecting local path.

  **QA Scenarios**:
  ```text
  Scenario: Dataproc baseline tests pass
    Tool: Bash
    Steps: Run dataproc mock contract suite.
    Expected Result: Baseline operations pass and assertions hold.
    Evidence: .sisyphus/evidence/task-16-dataproc-ci-happy.txt

  Scenario: Partition request returns explicit unsupported error
    Tool: Bash
    Steps: Invoke partition API path in baseline mode.
    Expected Result: Deterministic unsupported-feature error with provider+operation tags.
    Evidence: .sisyphus/evidence/task-16-dataproc-ci-error.txt
  ```

  **Commit**: YES
  - Message: `test(dataproc): add baseline matrix and unsupported contracts`

- **Phase 4 - Cross-Provider Hardening and Release Gates**
- **Scope IN**: Conformance validation, milestone checks, packaging hardening.
- **Scope OUT**: New provider capabilities beyond M0-M3 definitions.

- [ ] 17. Build cross-provider conformance suite

  **What to do**:
  - Add shared conformance tests validating attach, list, get, and scan behaviors across providers.
  - Verify consistent error taxonomy and debug output shapes.

  **Must NOT do**:
  - Do not add provider-specific branching in conformance assertions.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 4 (with T18-T19)
  - **Blocks**: T18
  - **Blocked By**: T5, T6, T7, T10, T13

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:408` - milestone framing to enforce across providers.
  - `test/sql/quack.test` - SQLLogic assertion style for baseline conformance cases.

  **Acceptance Criteria**:
  - [ ] Conformance suite passes across all implemented providers in supported modes.
  - [ ] Error-tag consistency check passes (`provider`, `operation`, `retryable`).

  **QA Scenarios**:
  ```text
  Scenario: Shared attach/list/get contract passes for all providers
    Tool: Bash
    Steps: Run conformance suite matrix.
    Expected Result: All provider rows pass required baseline assertions.
    Evidence: .sisyphus/evidence/task-17-conformance-happy.txt

  Scenario: Provider inconsistency detected and fails suite
    Tool: Bash
    Steps: Run suite against injected inconsistent error-tag fixture.
    Expected Result: Suite fails with provider + mismatch details.
    Evidence: .sisyphus/evidence/task-17-conformance-error.txt
  ```

  **Commit**: YES
  - Message: `test(metastore): add cross-provider conformance suite`

- [ ] 18. Execute milestone gate validation (M0-M3)

  **What to do**:
  - Validate each milestone exit criterion with concrete command checks and evidence.
  - Produce milestone status report for M0, M1, M2, M3.

  **Must NOT do**:
  - Do not mark milestones complete from qualitative judgment only.

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 4
  - **Blocks**: T19, FINAL
  - **Blocked By**: T11, T14, T16, T17

  **References**:
  - `METASTORE_EXTENSION_PLAN.md:410` - milestone definitions.
  - `.github/workflows/*.yml` - CI evidence sources for gate status.

  **Acceptance Criteria**:
  - [ ] Milestone report includes PASS/FAIL for M0-M3 with linked command outputs.
  - [ ] Any failed gate includes blocking issue and remediation item.

  **QA Scenarios**:
  ```text
  Scenario: All completed milestone gates evaluate correctly
    Tool: Bash
    Steps: Run milestone gate script collecting task evidence.
    Expected Result: Script prints status table for M0-M3 with deterministic verdicts.
    Evidence: .sisyphus/evidence/task-18-milestones-happy.txt

  Scenario: Missing prerequisite evidence blocks gate
    Tool: Bash
    Steps: Temporarily remove one evidence artifact and rerun gate script.
    Expected Result: Gate fails and reports missing artifact path.
    Evidence: .sisyphus/evidence/task-18-milestones-error.txt
  ```

  **Commit**: YES
  - Message: `chore(validation): add milestone gate verification`

- [ ] 19. Final release packaging and CI hardening checks

  **What to do**:
  - Ensure build/release pipeline includes provider feature flags and artifact checks.
  - Finalize release checklist for extension packaging and smoke loading.

  **Must NOT do**:
  - Do not broaden release scope to non-planned features.

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: `git-master`.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 4
  - **Blocks**: FINAL
  - **Blocked By**: T18

  **References**:
  - `extension_config.cmake` - extension packaging configuration anchor.
  - `Makefile` - build and artifact command conventions.

  **Acceptance Criteria**:
  - [ ] Release build job passes and emits extension artifact.
  - [ ] Smoke-load test validates produced artifact loads in DuckDB.

  **QA Scenarios**:
  ```text
  Scenario: Release artifact builds and loads
    Tool: Bash
    Steps: Run release target; load produced artifact in DuckDB shell.
    Expected Result: Artifact exists and loads without symbol errors.
    Evidence: .sisyphus/evidence/task-19-release-happy.txt

  Scenario: Missing packaging input fails early
    Tool: Bash
    Steps: Trigger build with intentionally missing packaging file.
    Expected Result: Build fails with clear missing-file diagnostics.
    Evidence: .sisyphus/evidence/task-19-release-error.txt
  ```

  **Commit**: YES
  - Message: `chore(release): harden packaging and smoke checks`

---

## Final Verification Wave (MANDATORY)

- [ ] F1. **Plan Compliance Audit** - `oracle`
  Verify each Must Have and Must NOT Have against implementation and evidence files.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT`

- [ ] F2. **Code Quality Review** - `unspecified-high`
  Run build, lint, and test commands; inspect changed files for quality and safety issues.
  Output: `Build [PASS/FAIL] | Lint [PASS/FAIL] | Tests [N pass/N fail] | VERDICT`

- [ ] F3. **Real QA Execution** - `unspecified-high`
  Execute all task QA scenarios end-to-end and save evidence under `.sisyphus/evidence/final-qa/`.
  Output: `Scenarios [N/N pass] | Integration [N/N] | Edge Cases [N tested] | VERDICT`

- [ ] F4. **Scope Fidelity Check** - `deep`
  Ensure task-by-task implementation matches plan and no unplanned scope was added.
  Output: `Tasks [N/N compliant] | Contamination [CLEAN/N issues] | VERDICT`

---

## Commit Strategy

- **Wave 1**: `feat(metastore-core): scaffold extension, contracts, auth chokepoint`
- **Wave 2**: `feat(hms): add hive adapter, mapping, and integration coverage`
- **Wave 3**: `feat(cloud-catalogs): add glue and dataproc baseline support`
- **Wave 4**: `chore(validation): add conformance, milestone gates, and release hardening`

---

## Success Criteria

### Verification Commands

```bash
test -f .sisyphus/plans/metastore-extension-execution-plan.md
python - <<'PY'
from pathlib import Path
p = Path('.sisyphus/plans/metastore-extension-execution-plan.md')
t = p.read_text()
required = [
    'Phase 0', 'Phase 1', 'Phase 2', 'Phase 3',
    'Scope IN', 'Scope OUT', 'Dependency Matrix',
    'Acceptance Criteria', 'QA Scenarios'
]
missing = [x for x in required if x not in t]
assert not missing, missing
print('plan-structure-ok')
PY
```

### Final Checklist
- [ ] All Must Have requirements mapped to tasks
- [ ] All Must NOT Have guardrails enforced
- [ ] All tasks include executable acceptance criteria
- [ ] All tasks include happy-path and negative QA scenarios
