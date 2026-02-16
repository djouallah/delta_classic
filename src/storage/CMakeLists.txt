include_directories(include)

add_library(delta_classic_ext_storage OBJECT
    delta_classic_catalog.cpp
    delta_classic_schema_entry.cpp
    delta_classic_table_entry.cpp
    delta_classic_table_set.cpp
    delta_classic_transaction.cpp
    delta_classic_transaction_manager.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:delta_classic_ext_storage>
    PARENT_SCOPE)
