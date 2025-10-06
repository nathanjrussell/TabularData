# tests/CMakeLists.txt
file(GLOB SAMPLE_CSV_FILES "${CMAKE_CURRENT_SOURCE_DIR}/sample_csv/*.csv")

add_executable(tabulardata_gtest tests/test_headers_gtest.cpp)
target_link_libraries(tabulardata_gtest PRIVATE
  TabularData
  gtest_main
)


# Copy sample CSVs to the binary directory for runtime
foreach(csv ${SAMPLE_CSV_FILES})
  configure_file(${csv} ${CMAKE_CURRENT_BINARY_DIR}/sample_csv/ COPYONLY)
endforeach()

include(GoogleTest)
gtest_discover_tests(tabulardata_gtest
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
