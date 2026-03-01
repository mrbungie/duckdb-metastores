add_library(hms_providers_lib OBJECT hms_connector.cpp hms_mapper.cpp)
# Link to hms_thrift_lib to get generated headers in include path
target_link_libraries(hms_providers_lib PRIVATE hms_thrift_lib)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:hms_providers_lib> PARENT_SCOPE)
