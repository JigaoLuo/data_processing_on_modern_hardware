cmake_minimum_required(VERSION 3.10)
project(hw6)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wpedantic -Wno-unused-parameter -march=native -lnuma")

find_package(Threads REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(EnableAddressSanitizer)
include(EnableUndefinedSanitizer)
include(BundledGTest)

include_directories(.)

add_executable(prj ParallelRadixJoins.cpp Relation.hpp perfEvent.hpp ThreadPool.hpp Barrier.hpp)
target_link_libraries(prj numa Threads::Threads ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})

add_executable(tester Tester.cpp TestHashJoin.cpp)
target_link_libraries(tester GTest::GTest numa Threads::Threads ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})
