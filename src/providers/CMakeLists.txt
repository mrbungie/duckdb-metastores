add_library(providers_lib OBJECT bridge_ffi.cpp)
add_subdirectory(hms)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:providers_lib>
    PARENT_SCOPE)
