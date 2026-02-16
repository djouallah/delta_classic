include_directories(include storage/include)
add_subdirectory(storage)

add_library(delta_classic_ext_library OBJECT delta_classic_extension.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:delta_classic_ext_library>
    PARENT_SCOPE)
