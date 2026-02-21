/*
 * metastore_ffi.h — Stable C ABI for metastore connector implementations.
 *
 * The Rust connector core implements these functions. The C++ bridge
 * (bridge_ffi.cpp) calls through them to satisfy IMetastoreConnector.
 *
 * Rules:
 *   - Pure C only — no C++ types, no exceptions, no templates.
 *   - Every allocated pointer has a corresponding _free function.
 *   - Error envelopes are returned by value (stack-allocated).
 *   - Opaque handle hides all implementation detail.
 */
#ifndef METASTORE_FFI_H
#define METASTORE_FFI_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/* Error codes                                                         */
/* ------------------------------------------------------------------ */
typedef enum {
	METASTORE_OK = 0,
	METASTORE_NOT_FOUND = 1,
	METASTORE_PERMISSION_DENIED = 2,
	METASTORE_TRANSIENT = 3,
	METASTORE_INVALID_CONFIG = 4,
	METASTORE_UNSUPPORTED = 5
} MetastoreFFIErrorCode;

/* ------------------------------------------------------------------ */
/* Error envelope — returned by every fallible FFI function             */
/* message and detail are owned by the caller; free with                */
/* metastore_error_free().                                             */
/* ------------------------------------------------------------------ */
typedef struct {
	int32_t code;
	const char *message;
	const char *detail;
	bool retryable;
} MetastoreFFIError;

void metastore_error_free(MetastoreFFIError *err);

/* ------------------------------------------------------------------ */
/* Opaque connector handle                                             */
/* ------------------------------------------------------------------ */
typedef struct MetastoreConnector_t *MetastoreConnectorHandle;

void metastore_connector_free(MetastoreConnectorHandle handle);

/* ------------------------------------------------------------------ */
/* String list — returned by list operations                           */
/* ------------------------------------------------------------------ */
typedef struct {
	const char **items;
	size_t count;
} MetastoreFFIStringList;

void metastore_string_list_free(MetastoreFFIStringList *list);

/* ------------------------------------------------------------------ */
/* Namespace — mirrors MetastoreNamespace on the C++ side               */
/* ------------------------------------------------------------------ */
typedef struct {
	const char *name;
	const char *catalog;
	const char *description;
	const char *location;
} MetastoreFFINamespace;

typedef struct {
	MetastoreFFINamespace *items;
	size_t count;
} MetastoreFFINamespaceList;

void metastore_namespace_list_free(MetastoreFFINamespaceList *list);

/* ------------------------------------------------------------------ */
/* Storage descriptor                                                  */
/* ------------------------------------------------------------------ */
typedef struct {
	const char *location;
	const char *format;
	const char *serde_class;
	const char *input_format;
	const char *output_format;
} MetastoreFFIStorageDescriptor;

/* ------------------------------------------------------------------ */
/* Partition column                                                    */
/* ------------------------------------------------------------------ */
typedef struct {
	const char *name;
	const char *type;
} MetastoreFFIPartitionColumn;

/* ------------------------------------------------------------------ */
/* Table — mirrors MetastoreTable on the C++ side                      */
/* ------------------------------------------------------------------ */
typedef struct {
	const char *catalog;
	const char *namespace_name;
	const char *name;
	MetastoreFFIStorageDescriptor storage_descriptor;
	MetastoreFFIPartitionColumn *partition_columns;
	size_t partition_column_count;
	const char **property_keys;
	const char **property_values;
	size_t property_count;
	const char *owner;
} MetastoreFFITable;

void metastore_table_free(MetastoreFFITable *table);

/* ------------------------------------------------------------------ */
/* Partition value                                                     */
/* ------------------------------------------------------------------ */
typedef struct {
	const char **values;
	size_t value_count;
	const char *location;
} MetastoreFFIPartitionValue;

typedef struct {
	MetastoreFFIPartitionValue *items;
	size_t count;
} MetastoreFFIPartitionValueList;

void metastore_partition_value_list_free(MetastoreFFIPartitionValueList *list);

/* ------------------------------------------------------------------ */
/* Key-value list — used for table stats / properties                  */
/* ------------------------------------------------------------------ */
typedef struct {
	const char **keys;
	const char **values;
	size_t count;
} MetastoreFFIKeyValueList;

void metastore_key_value_list_free(MetastoreFFIKeyValueList *list);

/* ------------------------------------------------------------------ */
/* Connector operations                                                */
/*                                                                     */
/* Each function writes its result into an out-pointer and returns an  */
/* error envelope. Caller checks error.code == METASTORE_OK before     */
/* reading the out-pointer. Caller owns the out-pointer contents and   */
/* must call the matching _free function.                              */
/* ------------------------------------------------------------------ */

MetastoreFFIError metastore_list_namespaces(MetastoreConnectorHandle handle,
                                            MetastoreFFINamespaceList *out);

MetastoreFFIError metastore_list_tables(MetastoreConnectorHandle handle,
                                        const char *namespace_name,
                                        MetastoreFFIStringList *out);

MetastoreFFIError metastore_get_table(MetastoreConnectorHandle handle,
                                      const char *namespace_name,
                                      const char *table_name,
                                      MetastoreFFITable *out);

MetastoreFFIError metastore_list_partitions(MetastoreConnectorHandle handle,
                                            const char *namespace_name,
                                            const char *table_name,
                                            const char *predicate,
                                            MetastoreFFIPartitionValueList *out);

MetastoreFFIError metastore_get_table_stats(MetastoreConnectorHandle handle,
                                            const char *namespace_name,
                                            const char *table_name,
                                            MetastoreFFIKeyValueList *out);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* METASTORE_FFI_H */
