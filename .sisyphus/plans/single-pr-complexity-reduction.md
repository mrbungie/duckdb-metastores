# Single-PR Complexity Reduction for Metastore Core

## TL;DR
> **Summary**: Reduce cyclomatic complexity in the highest-risk metastore hotspots in one PR, with strict no-behavior-change constraints and the same regression suites passing.
> **Deliverables**:
> - Refactored hotspot functions with extracted helpers/pipelines
> - Complexity delta evidence (before/after lizard snapshots)
> - Regression evidence for HMS and generic metastore suites
> **Effort**: Medium
> **Parallel**: YES - 3 waves
> **Critical Path**: Task 1 -> Task 2 -> Task 3 -> Task 8

## Context
### Original Request
User asked to measure `src/` complexity using tools and then requested a concrete proposal; user confirmed execution format: single PR and must pass the same tests.

### Interview Summary
- Target hotspots identified from tooling (lizard), especially `BindUnderlyingFunction`, `MetastoreInsertExecute`, and `PlanScan`.
- Constraints confirmed: one PR, no behavior changes, same test suites passing.

### Metis Review (gaps addressed)
- Added explicit guardrails against scope creep (no bug fixes/perf tuning/wording changes).
- Added baseline vs post-change complexity comparison requirements.
- Added explicit edge-case parity requirements for endpoint parsing, type mapping, partition pruning, and insert flows.
- Added environment-conditional HMS checks and fallback policy.

## Work Objectives
### Core Objective
Ship a single refactor PR that materially reduces hotspot complexity while preserving externally observable behavior.

### Deliverables
- Helper/pipeline extraction across selected hotspots.
- Verified complexity reduction for hotspot functions.
- Full regression evidence for metastore generic + HMS suites.
- One PR-ready commit sequence and summary.

### Definition of Done (verifiable conditions with commands)
- `lizard src/ --languages cpp -s cyclomatic_complexity --length 30` shows reduced CCN in hotspot targets with no hotspot CCN increase.
- `./build/release/test/unittest "test/sql/metastore/generic/*"` exits 0.
- `HMS_LOCAL_ENDPOINT=127.0.0.1:9083 ./build/release/test/unittest "test/sql/metastore/hms/*"` exits 0.
- `git diff --name-only` only contains planned scope files.

### Must Have
- Single PR implementation.
- Zero behavior changes (results, error class/messages, partition pruning decisions, scan file resolution semantics).
- Complexity reductions focused on top hotspots first.

### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- No feature additions, bug fixes, performance tuning, or formatting-only sweeps.
- No API contract changes for connector/provider behavior.
- No unrelated file churn outside selected scope.

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: tests-after + existing DuckDB unittest framework
- QA policy: Every task includes executable happy + failure/edge scenarios
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`
- Parity default: preserve exact error class and message text unless existing tests explicitly assert otherwise.
- HMS fallback policy: if HMS endpoint/env is unavailable, mark HMS gate as blocked with captured logs and continue non-HMS verification; final merge gate requires HMS pass.

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. <3 per wave (except final) = under-splitting.

Wave 1: Baseline contracts and complexity snapshots (Task 1)

Wave 2: Core hotspot reductions in parallel where independent (Tasks 2, 4, 5)

Wave 3: Secondary complexity cuts + parity hardening + final validation prep (Tasks 3, 6, 7, 8)

### Dependency Matrix (full, all tasks)
| Task | Blocks | Blocked By |
|---|---|---|
| 1 | 2,3,4,5,6,7,8 | - |
| 2 | 3,8 | 1 |
| 3 | 8 | 1,2 |
| 4 | 8 | 1 |
| 5 | 8 | 1 |
| 6 | 8 | 1 |
| 7 | 8 | 1 |
| 8 | Final Verification Wave | 2,3,4,5,6,7 |

### Agent Dispatch Summary (wave -> task count -> categories)
- Wave 1 -> 1 task -> `unspecified-low`
- Wave 2 -> 3 tasks -> `deep`, `unspecified-high`
- Wave 3 -> 4 tasks -> `unspecified-high`, `quick`

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [x] 1. Capture Baseline Contracts and Complexity Snapshot

  **What to do**: Record baseline lizard metrics and behavior invariants for hotspot functions before any refactor. Persist machine-readable before snapshot and hotspot contract notes (inputs/outputs/errors/order-of-operations).
  **Must NOT do**: Do not edit production code in this task.

  **Recommended Agent Profile**:
  - Category: `unspecified-low` - Reason: metrics and inventory only
  - Skills: `[]` - no specialized skill needed
  - Omitted: `playwright` - no browser/UI interaction

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 2,3,4,5,6,7,8 | Blocked By: -

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `src/functions/metastore_read.cpp` - hotspot location inventory
  - Pattern: `src/functions/metastore_write.cpp` - insert pipeline hotspot
  - Pattern: `src/metastore_scan_plan.cpp` - scan planning hotspot
  - Pattern: `src/providers/hms/hms_config.cpp` - endpoint parsing branches
  - Pattern: `src/metastore_utils.cpp` - Hive type mapping branches
  - Test: `test/sql/metastore/generic/` - generic regression suite
  - Test: `test/sql/metastore/hms/` - HMS regression suite

  **Acceptance Criteria** (agent-executable only):
  - [ ] `lizard src/ --languages cpp -s cyclomatic_complexity --length 30 > .sisyphus/evidence/task-1-baseline-lizard.txt` exits 0 and contains hotspot function names.
  - [ ] Baseline contract doc created at `.sisyphus/evidence/task-1-hotspot-contracts.md` with explicit invariants for Tasks 2-7.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path baseline capture
    Tool: Bash
    Steps: Run lizard command and verify output file exists and is non-empty.
    Expected: Evidence file includes BindUnderlyingFunction, MetastoreInsertExecute, PlanScan entries.
    Evidence: .sisyphus/evidence/task-1-baseline-lizard.txt

  Scenario: Failure/edge case missing binary
    Tool: Bash
    Steps: Run `command -v lizard`; if missing, install per environment policy and re-run capture.
    Expected: Final status still produces baseline report; if install unavailable, task marked blocked with error log.
    Evidence: .sisyphus/evidence/task-1-baseline-lizard-error.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: `.sisyphus/evidence/task-1-*`

- [x] 2. Refactor `BindUnderlyingFunction` into Composable Helpers

  **What to do**: In `src/functions/metastore_read.cpp`, extract helper functions for file list assembly, ignored-file filtering, format-function resolution, and named-parameter assembly. Keep call order and argument values identical.
  **Must NOT do**: Do not change format detection behavior, file glob semantics, or error strings.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: high-branching function with semantic coupling
  - Skills: `[]` - existing codebase patterns are sufficient
  - Omitted: `git-master` - no git archaeology needed

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: 3,8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `src/functions/metastore_read.cpp` - function `BindUnderlyingFunction`
  - Pattern: `src/formats/csv_reader.cpp` - CSV option wiring conventions
  - Pattern: `src/formats/parquet_reader.cpp` - parquet scan path conventions
  - Pattern: `src/formats/json_reader.cpp` - JSON reader conventions
  - API/Type: `src/include/metastore_types.hpp` - format enum/type expectations
  - Test: `test/sql/metastore/hms/hms_pushdown_csv.test` - CSV behavior guard
  - Test: `test/sql/metastore/hms/hms_pushdown_parquet.test` - parquet behavior guard
  - Test: `test/sql/metastore/hms/hms_pushdown_json.test` - JSON behavior guard

  **Acceptance Criteria** (agent-executable only):
  - [ ] Post-change lizard report shows `BindUnderlyingFunction` CCN <= 15.
  - [ ] `HMS_LOCAL_ENDPOINT=127.0.0.1:9083 ./build/release/test/unittest "test/sql/metastore/hms/*"` exits 0.
  - [ ] `./build/release/test/unittest "test/sql/metastore/generic/*"` exits 0.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path refactor parity
    Tool: Bash
    Steps: Rebuild unittest target; run HMS and generic metastore suites.
    Expected: All targeted tests pass with no changed expected output files.
    Evidence: .sisyphus/evidence/task-2-bind-refactor-tests.txt

  Scenario: Failure/edge case ignored sidecar files
    Tool: Bash
    Steps: Run `./build/release/test/unittest "test/sql/metastore/hms/hms_pushdown_csv.test"` and inspect failures if any around hidden/CRC files.
    Expected: No regression in hidden/sidecar filtering behavior.
    Evidence: .sisyphus/evidence/task-2-bind-refactor-edge.txt
  ```

  **Commit**: YES | Message: `refactor(read): decompose bind path without behavior change` | Files: `src/functions/metastore_read.cpp`

- [x] 3. Consolidate Partition Pruning Evaluation Paths

  **What to do**: In `src/functions/metastore_read.cpp`, extract shared evaluator utilities used by `PrunePartitionsByExpressions` and `PrunePartitionsByTableFilters`; remove duplicated branching while preserving pruning decisions.
  **Must NOT do**: Do not alter pruning semantics, null handling, or filter precedence.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: moderate complexity with correctness sensitivity
  - Skills: `[]` - no external docs required
  - Omitted: `playwright` - non-UI task

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: 8 | Blocked By: 1,2

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `src/functions/metastore_read.cpp` - pruning functions and filter rewrite logic
  - API/Type: `src/include/metastore_partition_predicate.hpp` - predicate model
  - Pattern: `src/metastore_partition_predicate.cpp` - existing predicate conversion rules
  - Test: `test/sql/metastore/hms/test_hms_partition_reads.test` - partition read correctness
  - Test: `test/sql/metastore/hms/hms_pushdown.test` - pushdown behavior

  **Acceptance Criteria** (agent-executable only):
  - [ ] `PrunePartitionsByExpressions` and `PrunePartitionsByTableFilters` both have lower CCN than baseline.
  - [ ] `./build/release/test/unittest "test/sql/metastore/hms/test_hms_partition_reads.test"` exits 0.
  - [ ] `./build/release/test/unittest "test/sql/metastore/hms/hms_pushdown.test"` exits 0.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path pruning parity
    Tool: Bash
    Steps: Run partition and pushdown tests after rebuild.
    Expected: Identical pass/fail outcomes to baseline (all pass).
    Evidence: .sisyphus/evidence/task-3-pruning-happy.txt

  Scenario: Failure/edge case contradictory predicates
    Tool: Bash
    Steps: Run test files that include contradictory filters and inspect output for empty/filtered results handling.
    Expected: No under-prune/over-prune regressions.
    Evidence: .sisyphus/evidence/task-3-pruning-edge.txt
  ```

  **Commit**: YES | Message: `refactor(read): unify partition pruning evaluation helpers` | Files: `src/functions/metastore_read.cpp`

- [x] 4. Decompose `PlanScan` into Deterministic Stages

  **What to do**: In `src/metastore_scan_plan.cpp`, split `PlanScan` into explicit stages: partition-name extraction, partition listing, file expansion/filtering, and scan target assembly.
  **Must NOT do**: Do not change partition ordering, file filtering policy, or selected scan file set semantics.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: medium-to-high branching with I/O path sensitivity
  - Skills: `[]` - code-local patterns sufficient
  - Omitted: `git-master` - no history mining required

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: 8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `src/metastore_scan_plan.cpp` - `PlanScan`, `PartitionNames`
  - API/Type: `src/include/metastore_scan_plan.hpp` - scan plan contracts
  - Pattern: `src/functions/metastore_read.cpp` - consumer expectations for `scan_files`
  - Test: `test/sql/metastore/hms/test_hms_partition_reads.test` - partition file resolution
  - Test: `test/sql/metastore/hms/hms_pushdown_parquet.test` - file-level scan correctness

  **Acceptance Criteria** (agent-executable only):
  - [ ] `PlanScan` CCN <= 12.
  - [ ] Partition-read and parquet pushdown tests exit 0.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path deterministic scan planning
    Tool: Bash
    Steps: Run partition-read/parquet tests twice in sequence.
    Expected: Both runs pass and produce stable deterministic outcomes.
    Evidence: .sisyphus/evidence/task-4-planscan-happy.txt

  Scenario: Failure/edge case hidden or sidecar file entries
    Tool: Bash
    Steps: Run HMS CSV/parquet pushdown tests that touch sidecar-filter behavior.
    Expected: No attempts to scan ignored hidden/sidecar files.
    Evidence: .sisyphus/evidence/task-4-planscan-edge.txt
  ```

  **Commit**: YES | Message: `refactor(scan): split PlanScan into staged helpers` | Files: `src/metastore_scan_plan.cpp`, `src/include/metastore_scan_plan.hpp`

- [ ] 5. Refactor `MetastoreInsertExecute` into Explicit Pipeline Steps

  **What to do**: In `src/functions/metastore_write.cpp`, extract pipeline functions for target resolution, write planning, physical write execution, and partition registration.
  **Must NOT do**: Do not alter write layout, registration order, or failure/error surface.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: high-complexity core write flow (CCN 32)
  - Skills: `[]` - local context sufficient
  - Omitted: `playwright` - no UI path

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: 8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `src/functions/metastore_write.cpp` - `MetastoreInsertExecute`
  - API/Type: `src/include/metastore_functions.hpp` - write function contracts
  - Pattern: `src/metastore_runtime.cpp` - connector/runtime usage patterns
  - Test: `test/sql/metastore/generic/metastore_full_flow.test` - end-to-end write/read flow
  - Test: `test/sql/metastore/generic/metastore_joins.test` - downstream read correctness

  **Acceptance Criteria** (agent-executable only):
  - [ ] `MetastoreInsertExecute` CCN <= 15.
  - [ ] `./build/release/test/unittest "test/sql/metastore/generic/*"` exits 0.
  - [ ] `HMS_LOCAL_ENDPOINT=127.0.0.1:9083 ./build/release/test/unittest "test/sql/metastore/hms/*"` exits 0.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path insert pipeline parity
    Tool: Bash
    Steps: Run generic full-flow tests after refactor.
    Expected: All insert/read path tests pass with unchanged expected outputs.
    Evidence: .sisyphus/evidence/task-5-insert-pipeline-happy.txt

  Scenario: Failure/edge case empty or invalid insert input
    Tool: Bash
    Steps: Run targeted metastore write tests that exercise invalid/edge insert inputs.
    Expected: Same error class/messages as baseline contracts.
    Evidence: .sisyphus/evidence/task-5-insert-pipeline-edge.txt
  ```

  **Commit**: YES | Message: `refactor(write): split insert execute flow into pipeline` | Files: `src/functions/metastore_write.cpp`

- [ ] 6. Reduce Branching in HMS Endpoint Parsing

  **What to do**: In `src/providers/hms/hms_config.cpp`, extract helpers for scheme stripping, host/port splitting, and port validation while preserving defaults and validation behavior.
  **Must NOT do**: Do not change accepted endpoint formats or emitted errors.

  **Recommended Agent Profile**:
  - Category: `quick` - Reason: constrained utility-level refactor
  - Skills: `[]` - codebase-local rules
  - Omitted: `librarian` - no external API decision needed

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: 8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `src/providers/hms/hms_config.cpp` - `ParseHmsEndpoint`, `ParsePort`
  - API/Type: `src/include/providers/hms/hms_config.hpp` - endpoint config contracts
  - Test: `test/sql/metastore/generic/metastore_connection.test` - connection behavior

  **Acceptance Criteria** (agent-executable only):
  - [ ] `ParseHmsEndpoint` CCN reduced from baseline.
  - [ ] Generic connection tests exit 0.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path endpoint forms
    Tool: Bash
    Steps: Run connection tests covering thrift endpoints and default port behavior.
    Expected: Endpoint normalization behavior matches baseline.
    Evidence: .sisyphus/evidence/task-6-hms-endpoint-happy.txt

  Scenario: Failure/edge case malformed endpoint
    Tool: Bash
    Steps: Execute tests with malformed endpoint forms from existing cases.
    Expected: Same validation failure behavior and error surface.
    Evidence: .sisyphus/evidence/task-6-hms-endpoint-edge.txt
  ```

  **Commit**: YES | Message: `refactor(hms): decompose endpoint parsing helpers` | Files: `src/providers/hms/hms_config.cpp`, `src/include/providers/hms/hms_config.hpp`

- [ ] 7. Convert Hive Type Mapping to Table-Driven Dispatch

  **What to do**: In `src/metastore_utils.cpp`, replace branch chain in `MapHiveTypeToDuckDB` with ordered table-driven dispatch and constrained parsing helpers for parameterized types.
  **Must NOT do**: Do not change mapping outputs for known and unknown Hive types.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: high semantic sensitivity in type mapping
  - Skills: `[]` - existing tests + local type contracts sufficient
  - Omitted: `playwright` - backend-only

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: 8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `src/metastore_utils.cpp` - `MapHiveTypeToDuckDB`
  - API/Type: `src/include/metastore_utils.hpp` - utility contracts
  - Pattern: `src/formats/csv_reader.cpp` - consumer of mapped types
  - Pattern: `src/formats/json_reader.cpp` - consumer of mapped types
  - Pattern: `src/functions/metastore_read.cpp` - schema binding consumer
  - Test: `test/sql/metastore/generic/test_metastore_formats.test` - format/type behavior

  **Acceptance Criteria** (agent-executable only):
  - [ ] `MapHiveTypeToDuckDB` CCN reduced from baseline.
  - [ ] `./build/release/test/unittest "test/sql/metastore/generic/test_metastore_formats.test"` exits 0.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path common Hive types
    Tool: Bash
    Steps: Run format/type tests and capture output.
    Expected: Known primitive and parameterized type mappings match baseline outputs.
    Evidence: .sisyphus/evidence/task-7-type-mapping-happy.txt

  Scenario: Failure/edge case unknown/complex types
    Tool: Bash
    Steps: Run tests covering unsupported/complex types.
    Expected: Fallback/error behavior unchanged.
    Evidence: .sisyphus/evidence/task-7-type-mapping-edge.txt
  ```

  **Commit**: YES | Message: `refactor(types): table-drive hive type mapping logic` | Files: `src/metastore_utils.cpp`, `src/include/metastore_utils.hpp`

- [ ] 8. Final Parity Verification, Complexity Delta, and PR Assembly

  **What to do**: Produce after snapshot, compute hotspot CCN deltas versus Task 1 baseline, run full regression suites, and prepare single-PR summary/evidence bundle.
  **Must NOT do**: Do not introduce new code changes except minimal fixes strictly required to satisfy no-regression criteria.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: verification-heavy integration gate
  - Skills: `[]` - standard bash/test workflow
  - Omitted: `frontend-ui-ux` - irrelevant domain

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: Final Verification Wave | Blocked By: 2,3,4,5,6,7

  **References** (executor has NO interview context - be exhaustive):
  - Evidence: `.sisyphus/evidence/task-1-baseline-lizard.txt` - before snapshot
  - Pattern: `.sisyphus/plans/single-pr-complexity-reduction.md` - target CCN thresholds
  - Test: `test/sql/metastore/generic/` - generic suite command scope
  - Test: `test/sql/metastore/hms/` - HMS suite command scope

  **Acceptance Criteria** (agent-executable only):
  - [ ] `lizard src/ --languages cpp -s cyclomatic_complexity --length 30 > .sisyphus/evidence/task-8-after-lizard.txt` exits 0.
  - [ ] Delta report at `.sisyphus/evidence/task-8-ccn-delta.md` shows Task 2/4/5 target thresholds met and no hotspot CCN increase.
  - [ ] Generic and HMS test suite commands exit 0.
  - [ ] PR checklist document generated at `.sisyphus/evidence/task-8-pr-checklist.md` with file scope + evidence links.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```bash
  Scenario: Happy path full verification gate
    Tool: Bash
    Steps: Run after-lizard snapshot, then run generic and HMS suite commands.
    Expected: All commands exit 0 and delta report confirms complexity reduction targets.
    Evidence: .sisyphus/evidence/task-8-verification-happy.txt

  Scenario: Failure/edge case regression introduced
    Tool: Bash
    Steps: If any suite fails, capture failing test names and map to last touched task; apply minimal corrective refactor and rerun full gate.
    Expected: Final rerun passes both suites; corrective changes remain in planned scope only.
    Evidence: .sisyphus/evidence/task-8-verification-edge.txt
  ```

  **Commit**: YES | Message: `test(refactor): verify complexity reductions and regression parity` | Files: `.sisyphus/evidence/task-8-*`

## Final Verification Wave (4 parallel agents, ALL must APPROVE)
- [ ] F1. Plan Compliance Audit - oracle
- [ ] F2. Code Quality Review - unspecified-high
- [ ] F3. Real Manual QA - unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check - deep

## Commit Strategy
- Single PR, atomic commits by concern, squash at merge only if repository policy prefers.
- Commit message style: `refactor(metastore): reduce hotspot complexity without behavior change`.
- Include complexity/report evidence paths in PR body.

## Success Criteria
- Hotspot CCN reduced to target levels without semantic regressions.
- Same HMS + generic test suites pass.
- Review feedback indicates improved readability and maintainability.
