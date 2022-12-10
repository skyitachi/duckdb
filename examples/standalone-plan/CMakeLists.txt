cmake_minimum_required(VERSION 2.8.12)
project(example-plan)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

include_directories(../../src/include)
link_directories(../../cmake-build-debug/src)

add_executable(standalone-plan main.cpp)
target_link_libraries(standalone-plan duckdb)
