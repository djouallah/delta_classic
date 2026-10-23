cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME delta_classic)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD
    "17"
    CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include src/storage/include)

set(EXTENSION_SOURCES
    src/delta_classic_extension.cpp
    src/storage/delta_classic_catalog.cpp
    src/storage/delta_classic_path.cpp
    src/storage/delta_classic_schema_entry.cpp
    src/storage/delta_classic_table_entry.cpp
    src/storage/delta_classic_table_set.cpp
    src/storage/delta_classic_transaction.cpp
    src/storage/delta_classic_transaction_manager.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
