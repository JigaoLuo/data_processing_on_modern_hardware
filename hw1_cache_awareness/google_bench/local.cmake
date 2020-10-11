add_executable(google_bm ${CMAKE_SOURCE_DIR}/google_bench/google_bm.cc)

target_link_libraries(
    google_bm
    lab
    benchmark
    gflags
    Threads::Threads)
