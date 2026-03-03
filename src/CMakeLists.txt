add_library(
  metastore_src_lib OBJECT 
  metastore_extension.cpp 
  metastore_runtime.cpp
  metastore_secret_bridge.cpp
  metastore_types.cpp
  metastore_partition_predicate.cpp
  metastore_replacement_scan.cpp 
  metastore_storage_extension.cpp
  metastore_utils.cpp
  metastore_scan_plan.cpp
  functions/metastore_table_info.cpp 
  functions/metastore_read.cpp
  metastore_functions.cpp
  providers/hms/hms_connector.cpp
  providers/hms/hms_mapper.cpp
  providers/hms/hms_config.cpp
  providers/hms/hms_registration.cpp
  providers/mock/mock_connector.cpp
  providers/mock/mock_metastore.cpp
)

target_link_libraries(metastore_src_lib PRIVATE hms_thrift_lib)

set(METASTORE_EXTENSION_OBJECTS
    $<TARGET_OBJECTS:metastore_src_lib>
    PARENT_SCOPE)