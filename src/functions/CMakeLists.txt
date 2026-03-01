add_library(functions_lib OBJECT metastore_read.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:functions_lib>
    PARENT_SCOPE)
