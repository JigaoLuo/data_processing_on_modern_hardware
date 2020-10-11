cmake_minimum_required(VERSION 3.10)
project(hw4)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Wno-unused-parameter")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native") # Use AVX
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=skylake-avx512")

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(EnableAddressSanitizer)
include(EnableUndefinedSanitizer)
include(BundledGTest)

file(GLOB SRC ${PROJECT_SOURCE_DIR}/*.hpp ${PROJECT_SOURCE_DIR}/*.cpp)

add_executable(agg aggregation.cpp Util.cpp)
target_link_libraries(agg)

add_executable(dic dictionaryCompression.cpp Util.cpp)
target_link_libraries(dic)

add_executable(tester Tester.cpp TestAggregation.cpp TestDictionaryCompression.cpp Util.cpp)
target_link_libraries(tester GTest::GTest)
