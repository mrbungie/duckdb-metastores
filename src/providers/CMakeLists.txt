add_subdirectory(hms)
# Only HMS is currently supported, bridge_ffi.cpp is unused
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)
