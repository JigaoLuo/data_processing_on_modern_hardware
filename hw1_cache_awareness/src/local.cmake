# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------

set(SRC_CC_LINTER_IGNORE "")
# include("${CMAKE_SOURCE_DIR}/src/this-could-be-your-folder/local.cmake")

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

file(GLOB_RECURSE SRC_CC src/*.cc src/*.c)

# Gather lintable files
set(SRC_CC_LINTING "")
foreach(SRC_FILE ${SRC_CC})
    list(FIND SRC_CC_LINTER_IGNORE "${SRC_FILE}" SRC_FILE_IDX)
    if (${SRC_FILE_IDX} EQUAL -1)
        list(APPEND SRC_CC_LINTING "${SRC_FILE}")
    endif()
endforeach()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

add_library(lab STATIC ${SRC_CC} ${INCLUDE_H})
target_link_libraries(lab gflags Threads::Threads)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_cpplint_target(lint_src "${SRC_CC_LINTING}")
list(APPEND lint_targets lint_src)

# ---------------------------------------------------------------------------
# PerfEvent
# ---------------------------------------------------------------------------

add_executable(perfevent ${CMAKE_SOURCE_DIR}/src/PerfEvent.cc)
target_link_libraries(
        perfevent
        lab
        gflags)

# ---------------------------------------------------------------------------
# CycleCounter
# ---------------------------------------------------------------------------

add_executable(cyclecounter ${CMAKE_SOURCE_DIR}/src/CycleCounter.c)
target_link_libraries(
        cyclecounter
        lab
        gflags)

