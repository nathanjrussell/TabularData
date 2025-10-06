cmake_minimum_required(VERSION 3.16)
project(TabularData LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Chunk size macro
add_compile_definitions(CHUNK_SIZE=1048576)

# Library
add_library(TabularData src/TabularData.cpp)
target_include_directories(TabularData PUBLIC include)

# Example app
add_executable(main examples/main.cpp)
target_link_libraries(main PRIVATE TabularData)

# ---- GoogleTest ----
option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    )
    FetchContent_MakeAvailable(googletest)

    enable_testing()
    include(GoogleTest)

    add_executable(test_headers tests/test_headers_gtest.cpp)
    target_link_libraries(test_headers PRIVATE TabularData gtest_main)
    target_compile_definitions(test_headers PRIVATE CHUNK_SIZE=16)
    gtest_discover_tests(test_headers)
endif()

