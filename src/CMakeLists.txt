include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)


add_subdirectory(auth)
add_subdirectory(functions)
add_subdirectory(planner)
add_subdirectory(providers)

add_library(metastore_src_lib OBJECT metastore_extension.cpp metastore_functions.cpp metastore_runtime.cpp)
target_link_libraries(metastore_src_lib PRIVATE hms_thrift_lib)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:metastore_src_lib> PARENT_SCOPE)
