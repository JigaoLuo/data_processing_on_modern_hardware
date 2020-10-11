cmake_minimum_required(VERSION 3.10)
project(hw2)

find_package(Threads) # -pthread
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -fPIC -Wall -W -DNDEBUG -Wno-unused-parameter")

file(GLOB SRC ${PROJECT_SOURCE_DIR}/*.h ${PROJECT_SOURCE_DIR}/*.cpp)
add_executable(weedb ${SRC} PerfEvent.hpp)

