# DuckDB Metastore Extension Architecture

This document describes the current software architecture of the DuckDB Metastore extension, focusing on the patterns used, interactions between generic components and implementations, and entrypoints.

## Entrypoints

The extension hooks into duckdb primarily through two mechanisms defined in `src/metastore_extension.cpp`:

1. **Storage Extension (`StorageExtension`) & `MetastoreAttach`**:
   - Enables `ATTACH 'thrift://localhost:9083' AS ms;` 
   - `MetastoreAttach` captures the initialization options and connection URL. It parses the provider details and persists a `MetastoreConnectorConfig` in a global runtime cache for the catalog name.
   - It delegates to a fully in-memory `DuckCatalog` under the hood.

2. **Replacement Scans (`MetastoreReplacementScan`)**:
   - Acts as the automatic resolution hook when querying an unknown table (e.g., `SELECT * FROM ms.db.my_table`).
   - Retrieves the configuration for the catalog, constructs the metastore connector, and fetches the generic `MetastoreTable` metadata.
   - If the table is partitioned, it replaces the scan with a call to the `metastore_read` table function. If unpartitioned, it rewrites the table reference directly to native functions like `read_parquet(...)` or `read_csv_auto(...)` using the location provided by the metastore.

3. **Table Functions**:
   - `metastore_scan`: A utility function to retrieve table details layout (catalog, schema, table_name, location, format).
   - `metastore_read`: The core reading engine used to handle partitioned tables. It leverages `MetastorePlanner` to push down partition filters, then maps partitions into DuckDB's Multi-File Readers.

## Abstraction Layers (Generic vs. Implementation-Specific)

The architecture is cleanly divided between the **Generic Core** and the **Provider Implementations**.

### 1. Generic Core (`src/include/main`, `src/include/planner`)

- **Domain Models (`MetastoreTable`, `MetastoreNamespace`, `MetastorePartitionSpec`, etc.)**: These are provider-agnostic data structures that represent data catalogs. 
- **`IMetastoreConnector`**: The central interface for metastore backends. Every provider must implement this interface, which exposes methods like `ListNamespaces()`, `ListTables()`, `GetTable()`, and `ListPartitions()`. Responses are returned via a generic `MetastoreResult<T>` wrapper.
- **`MetastorePlanner`**: Evaluates `TableFilter` predicates pushed down by DuckDB, identifies partition predicates, and translates them into strings or representations that the underlying connector can push to the remote metastore.

### 2. Provider Implementations (`src/providers/hms`)

- **`HmsConnector`**: The concrete implementation of `IMetastoreConnector` for the Hive Metastore. It handles actual Thrift network communication.
- **`hms_mapper.cpp`**: Performs the critical translation between Provider-Specific Models (e.g. Apache Thrift's `Table` layout) and Generic Core Models (e.g., `MetastoreTable`).

### 3. Interactions

The interaction flows in a single direction during query execution:

1. **(Core) Query Execution** -> **(Generic) Replacement Scan / Table Function**: Determines what needs to be read.
2. **(Generic) Execution** -> **(Generic) `IMetastoreConnector`**: Calls `.GetTable()` or `.ListPartitions(predicate)`.
3. **(Implementation) `HmsConnector`**: Communicates with the external HMS instance.
4. **(Implementation) `HmsConnector`**: Maps the result via `hms_mapper` to generic structs and returns `MetastoreResult`.
5. **(Generic) Execution**: Returns the parsed files back into DuckDB's native scanning utilities.

## Assessment

**The overall abstraction is solid and follows good decoupling practices.**

The system properly establishes a generic interface (`IMetastoreConnector`) and canonical metastore models. The planner and the replacement scan operate purely on these generic structs, making the majority of the code immune to the specific metastore backing it. 

**Areas for Improvement:**
While the interface level abstractions are great, the *instantiation* layer is currently tightly coupled. In `metastore_extension.cpp` and `metastore_read.cpp`, the code manually checks `if (provider != MetastoreProviderType::HMS)` and directly instantiates `make_uniq<HmsConnector>(...)`. 

To achieve full decoupling (and seamlessly drop in Glue, Dataproc, or Iceberg REST support), the architecture should introduce a **ConnectorFactory** or Provider Registry pattern. When `ATTACH` is executed, the Registry should yield the correct instantiation of `IMetastoreConnector`, meaning that the generic core code (Replacement Scans and Generic Table Functions) would never need to mention "HMS" explicitly.

## Class Location Reference

| Class / Struct | Location | Description |
|---|---|---|
| `MetastoreScanBindData` | `src/metastore_functions.cpp` | |
| `MetastoreScanGlobalState` | `src/metastore_functions.cpp` | |
| `MetastoreReadBindData` | `src/functions/metastore_read.cpp` | |
| `MetastoreReadGlobalState` | `src/functions/metastore_read.cpp` | |
| `MetastoreReadLocalState` | `src/functions/metastore_read.cpp` | |
| `MetastoreColumn` | `src/include/main/metastore_types.hpp` | |
| `MetastoreStorageDescriptor` | `src/include/main/metastore_types.hpp` | |
| `MetastorePartitionColumn` | `src/include/main/metastore_types.hpp` | |
| `MetastorePartitionSpec` | `src/include/main/metastore_types.hpp` | |
| `MetastorePartitionValue` | `src/include/main/metastore_types.hpp` | |
| `MetastoreCatalog` | `src/include/main/metastore_types.hpp` | |
| `MetastoreNamespace` | `src/include/main/metastore_types.hpp` | |
| `MetastoreTable` | `src/include/main/metastore_types.hpp` | |
| `MetastoreDiagnosticInfo` | `src/include/main/metastore_errors.hpp` | |
| `MetastoreErrorTag` | `src/include/main/metastore_errors.hpp` | |
| `MetastoreException` | `src/include/main/metastore_errors.hpp` | |
| `HmsClientContext` | `src/providers/hms/hms_connector.cpp` | |
| `MetastorePartitionPredicate` | `src/include/planner/metastore_planner.hpp` | |
| `MetastoreScanFilter` | `src/include/planner/metastore_planner.hpp` | |
| `MetastorePlannerResult` | `src/include/planner/metastore_planner.hpp` | |
| `MetastorePlanner` | `src/include/planner/metastore_planner.hpp` | |
| `MetastoreExtension` | `src/include/metastore_extension.hpp` | |
| `MetastoreError` | `src/include/main/metastore_connector.hpp` | |
| `MetastoreResult` | `src/include/main/metastore_connector.hpp` | |
| `IMetastoreConnector` | `src/include/main/metastore_connector.hpp` | |
| `MetastoreConnectorConfig` | `src/include/auth/metastore_secret_bridge.hpp` | |
| `HmsRetryPolicy` | `src/include/providers/hms/hms_retry.hpp` | |
| `HmsConfig` | `src/include/providers/hms/hms_config.hpp` | |
| `HmsMapper` | `src/include/providers/hms/hms_mapper.hpp` | |
| `HmsConnector` | `src/include/providers/hms/hms_connector.hpp` | |

