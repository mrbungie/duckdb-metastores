add_library(auth_lib OBJECT metastore_secret_bridge.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:auth_lib>
    PARENT_SCOPE)
