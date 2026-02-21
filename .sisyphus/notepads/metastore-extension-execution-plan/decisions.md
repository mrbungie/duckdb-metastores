# Decisions

## [2026-02-21] Session ses_37ef4b4c7ffe4M2FY7wrURNAb3 - Wave 1

### Architecture: Hybrid (C++ shell + Rust core)
Decision: Follow METASTORE_EXTENSION_PLAN.md recommendation.
Rationale: Leverages strong Rust cloud SDK ecosystem; keeps DuckDB integration ergonomic.

### Extension Rename Strategy
Decision: Rename quack → metastore in all files as part of T1.
Rationale: This repo IS the target repo (extension-template). T1 must produce renamed extension shell.

### Wave 1 Parallelism
Decision: Launch T1, T2, T4, T6 in parallel (T3 waits for T2, T5 waits for T1+T2).
Actual parallel set: T1 || T2 — then T3, T4, T5, T6 can proceed as deps are met.
Simplification for execution: All 6 can be delegated in parallel since they write to different files/directories.
