add_library(
  metastore_src_lib OBJECT 
  metastore_extension.cpp 
  metastore_runtime.cpp
  metastore_secret_bridge.cpp
  metastore_types.cpp
  metastore_planner.cpp
  metastore_replacement_scan.cpp 
  metastore_storage_extension.cpp
  metastore_utils.cpp
  functions/metastore_scan.cpp 
  functions/metastore_read.cpp
  providers/hms/hms_connector.cpp
  providers/hms/hms_mapper.cpp
  providers/hms/hms_config.cpp
  providers/hms/hms_registration.cpp
)

target_link_libraries(metastore_src_lib PRIVATE hms_thrift_lib)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:metastore_src_lib>
    PARENT_SCOPE)