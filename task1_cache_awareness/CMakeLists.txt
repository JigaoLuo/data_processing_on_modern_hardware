cmake_minimum_required(VERSION 3.7)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)

include("${CMAKE_SOURCE_DIR}/vendor/benchmark.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/googletest.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/gflags.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/cpplint.cmake")

# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/include/local.cmake")
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${GTEST_INCLUDE_DIR}
    ${GFLAGS_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/src/local.cmake")

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/test/local.cmake")

# ---------------------------------------------------------------------------
# Google Benchmarks
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/google_bench/local.cmake")

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_custom_target(lint)
add_dependencies(lint ${lint_targets})
