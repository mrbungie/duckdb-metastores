add_library(planner_lib OBJECT metastore_planner.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:planner_lib> PARENT_SCOPE)
