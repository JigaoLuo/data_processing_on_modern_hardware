# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

file(GLOB_RECURSE TEST_CC test/*.cc)
list(REMOVE_ITEM TEST_CC test/tester.cc)

# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

add_executable(tester test/tester.cc ${TEST_CC})
target_link_libraries(tester lab gtest gmock Threads::Threads)

enable_testing()
add_test(lab tester)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_cpplint_target(lint_test "${TEST_CC}")
list(APPEND lint_targets lint_test)
