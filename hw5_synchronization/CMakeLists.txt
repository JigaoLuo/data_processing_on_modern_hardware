cmake_minimum_required(VERSION 3.10)
project(hw5)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wpedantic -Wno-unused-parameter -march=native")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fPIE -pie -g")

find_package(Threads REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
#include(EnableAddressSanitizer)
#include(EnableUndefinedSanitizer)
include(BundledGTest)

file(GLOB SRC ${PROJECT_SOURCE_DIR}/*.hpp ${PROJECT_SOURCE_DIR}/*.cpp)

add_executable(sync synchronization.cpp)
target_link_libraries(sync tbb Threads::Threads ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}  ${TBB_IMPORTED_TARGETS})

add_executable(tester Tester.cpp TestSynchronization.cpp)
target_link_libraries(tester GTest::GTest tbb Threads::Threads ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}  ${TBB_IMPORTED_TARGETS})
