add_library(
  metastore_src_lib OBJECT 
  metastore_extension.cpp 
  runtime/metastore_runtime.cpp
  core/connector/metastore_secret_bridge.cpp
  core/models/metastore_types.cpp
  core/planner/metastore_planner.cpp
  duckdb/sql/replacement_scan.cpp 
  duckdb/sql/storage_extension.cpp
  duckdb/metastore_utils.cpp
  duckdb/table_functions/metastore_scan.cpp 
  duckdb/table_functions/metastore_read.cpp
  providers/hms/hms_connector.cpp
  providers/hms/hms_mapper.cpp
  providers/hms/hms_config.cpp
  providers/hms/registration.cpp
)

target_link_libraries(metastore_src_lib PRIVATE hms_thrift_lib)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:metastore_src_lib>
    PARENT_SCOPE)


