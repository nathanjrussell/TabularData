cmake_minimum_required(VERSION 3.16)

# ---- Project ----
project(Tabulardata
  VERSION 0.1.0
  DESCRIPTION "Streaming, low-memory CSV header scanner and tabular utilities"
  LANGUAGES CXX)

# ---- Options (can be overridden by the consumer) ----
# Example override:
#   -DTABULARDATA_MAX_BUFFER_BYTES=4194304 -DTABULARDATA_NUM_THREADS=8
set(TABULARDATA_MAX_BUFFER_BYTES "1048576" CACHE STRING
    "Maximum bytes to use for the read buffer (header scan).")
set(TABULARDATA_NUM_THREADS "4" CACHE STRING
    "Default number of threads TabularData should use.")

option(TABULARDATA_BUILD_SHARED "Build Tabulardata as a shared library" ON)

# ---- Library ----
# Expect this layout:
#   include/TabularData.h
#   src/TabularData.cpp
set(_hdrs
  ${CMAKE_CURRENT_SOURCE_DIR}/include/TabularData.h
)
set(_srcs
  ${CMAKE_CURRENT_SOURCE_DIR}/src/TabularData.cpp
)

if(TABULARDATA_BUILD_SHARED)
  add_library(tabulardata SHARED ${_srcs} ${_hdrs})
else()
  add_library(tabulardata STATIC ${_srcs} ${_hdrs})
endif()

add_library(Tabulardata::tabulardata ALIAS tabulardata)

add_executable(tabular_demo ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
target_link_libraries(tabular_demo PRIVATE Tabulardata::tabulardata)

target_compile_features(tabulardata PUBLIC cxx_std_17)

# Export public include dir for both build and install consumers
target_include_directories(tabulardata
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Expose overridable macros to consumers (defaults provided above)
target_compile_definitions(tabulardata
  PUBLIC
    TABULARDATA_MAX_BUFFER_BYTES=${TABULARDATA_MAX_BUFFER_BYTES}
    TABULARDATA_NUM_THREADS=${TABULARDATA_NUM_THREADS}
)

# Reasonable defaults
set_target_properties(tabulardata PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  OUTPUT_NAME "tabulardata"
  CXX_EXTENSIONS OFF
  SOVERSION ${PROJECT_VERSION_MAJOR}
  VERSION ${PROJECT_VERSION}
)

# ---- Install (optional but nice to have) ----
include(GNUInstallDirs)

install(TARGETS tabulardata
  EXPORT TabulardataTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Generate an export file so find_package can work if you later add a Config.cmake
install(EXPORT TabulardataTargets
  NAMESPACE Tabulardata::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Tabulardata
)

# ---- Developer convenience ----
# Optionally, enable stricter warnings when building this project directly
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  if(MSVC)
    target_compile_options(tabulardata PRIVATE /W4 /permissive-)
  else()
    target_compile_options(tabulardata PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endif()

