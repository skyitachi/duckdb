cmake_minimum_required(VERSION 2.8.12)

project(HDFSExtension)

add_extension_definitions()

include_directories(include ../..)

add_library(hdfs_extension STATIC hdfs-extension.cpp)

set(PARAMETERS "-warnings")

build_loadable_extension(hdfs ${PARAMETERS} hdfs-extension.cpp)

target_link_libraries(hdfs_loadable_extension duckdb_mbedtls)

install(
        TARGETS hdfs_extension
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")