cmake_minimum_required(VERSION 3.10)
project(hw3)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Wno-unused-parameter")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native") # Use AVX

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(EnableAddressSanitizer)
include(EnableUndefinedSanitizer)
include(BundledGTest)

file(GLOB SRC ${PROJECT_SOURCE_DIR}/*.h ${PROJECT_SOURCE_DIR}/*.cpp)

add_executable(ohj ohj.cpp OptimizedHashJoins.hpp )
target_link_libraries(ohj rt)

add_executable(tester Tester.cpp TestHashJoin.cpp)
target_link_libraries(tester GTest::GTest)
