#include "ListBasedSetNoSync.hpp"
#include "ListBasedSetSync.hpp"

#include <utility>
#include <random>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <iostream>
#include <vector>
#include <tbb/tbb.h>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
struct SynchronizationTest: public ::testing::Test {
protected:

    void SetUp() override { return; }

    // Vector containing the number of threads for the experiments
    static constexpr uint32_t threads  = 10;

    // Vector containing the dataset
    vector<uint32_t> work;

    // Initialize the list-based set
    ListBasedSetNoSync<int64_t> ns;
    ListBasedSetCoarseLock<int64_t, tbb::mutex> cl;
    ListBasedSetCoarseLockRW<int64_t, tbb::spin_rw_mutex> crw;
    ListBasedSetLockCoupling<int64_t, tbb::mutex> lc;
    ListBasedSetLockCouplingRW<int64_t, tbb::spin_rw_mutex> lcrw;
    ListBasedSetOptimistic<int64_t, tbb::mutex> o;
    ListBasedSetOptimisticLockCoupling<int64_t, tbb::spin_rw_mutex> olc;
};
//---------------------------------------------------------------------------
TEST_F(SynchronizationTest, Contains_Dense) {
    /// Setup

    /// Dense, high contention
    // Number of (non-unique) values, which are inserted into the list. Feel free to change it
    static constexpr uint32_t operations = 10e6;
    // Define the domain range of the values.
    static constexpr uint32_t domain = 16;

    work.resize(operations);
    for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

    // Init list-based set. Insert only a small subset into the list-based set.
    for (uint32_t i = 0; i < domain * 0.75; i++) {
        const int64_t value = random() % domain;
        ns.insert(value);
        cl.insert(value);
        crw.insert(value);
        lc.insert(value);
        lcrw.insert(value);
        o.insert(value);
        olc.insert(value);
    }

    /// Real Tests
    tbb::task_scheduler_init taskScheduler;

    // ListBasedSetNoSync
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total(0);
    auto thread_contains = [&](const tbb::blocked_range<uint32_t> &range) {
        // Count the number of keys contained in the list-based set
        uint32_t sum = 0;
        for (uint32_t i = range.begin(); i < range.end(); i++)
            sum += ns.contains(work[i]);
        // Add the thread's matches to the global counter
        total += sum;
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains);
    std::cout << "total: " << total << std::endl;


    // ListBasedSetCoarseLock
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_cl(0);
        auto thread_contains_cl = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += cl.contains(work[i]);
            // Add the thread's matches to the global counter
            total_cl += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_cl);
        EXPECT_EQ(total, total_cl);
    }


    // ListBasedSetCoarseLockRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_crw(0);
        auto thread_contains_crw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += crw.contains(work[i]);
            // Add the thread's matches to the global counter
            total_crw += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_crw);
        EXPECT_EQ(total, total_crw);
    }


    // ListBasedSetLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lc(0);
        auto thread_contains_lc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += lc.contains(work[i]);
            // Add the thread's matches to the global counter
            total_lc += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_lc);
        EXPECT_EQ(total, total_lc);
    }


    // ListBasedSetLockCouplingRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lcrw(0);
        auto thread_contains_lcrw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += lcrw.contains(work[i]);
            // Add the thread's matches to the global counter
            total_lcrw += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_lcrw);
        EXPECT_EQ(total, total_lcrw);
    }

    // ListBasedSetOptimistic
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_o(0);
        auto thread_contains_o = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += o.contains(work[i]);
            // Add the thread's matches to the global counter
            total_o += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_o);
        EXPECT_EQ(total, total_o);
    }

    // ListBasedSetOptimisticLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_olc(0);
        auto thread_contains_olc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += olc.contains(work[i]);
            // Add the thread's matches to the global counter
            total_olc += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_olc);
        EXPECT_EQ(total, total_olc);
    }
}
//---------------------------------------------------------------------------
TEST_F(SynchronizationTest, Contains_Sparse) {
    /// Setup

    /// Sparse, low contention
    // Number of (non-unique) values, which are inserted into the list. Feel free to change it
    static constexpr uint32_t operations = 1024;
    // Define the domain range of the values.
    static constexpr uint32_t domain = 4028;

    work.resize(operations);
    for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

    // Init list-based set. Insert only a small subset into the list-based set.
    for (uint32_t i = 0; i < domain * 0.75; i++) {
        const int64_t value = random() % domain;
        ns.insert(value);
        cl.insert(value);
        crw.insert(value);
        lc.insert(value);
        lcrw.insert(value);
        o.insert(value);
        olc.insert(value);
    }

    /// Real Tests
    tbb::task_scheduler_init taskScheduler;

    // ListBasedSetNoSync
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total(0);
    auto thread_contains = [&](const tbb::blocked_range<uint32_t> &range) {
        // Count the number of keys contained in the list-based set
        uint32_t sum = 0;
        for (uint32_t i = range.begin(); i < range.end(); i++)
            sum += ns.contains(work[i]);
        // Add the thread's matches to the global counter
        total += sum;
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains);
    std::cout << "total: " << total << std::endl;


    // ListBasedSetCoarseLock
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_cl(0);
        auto thread_contains_cl = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += cl.contains(work[i]);
            // Add the thread's matches to the global counter
            total_cl += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_cl);
        EXPECT_EQ(total, total_cl);
    }


    // ListBasedSetCoarseLockRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_crw(0);
        auto thread_contains_crw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += crw.contains(work[i]);
            // Add the thread's matches to the global counter
            total_crw += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_crw);
        EXPECT_EQ(total, total_crw);
    }


    // ListBasedSetLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lc(0);
        auto thread_contains_lc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += lc.contains(work[i]);
            // Add the thread's matches to the global counter
            total_lc += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_lc);
        EXPECT_EQ(total, total_lc);
    }


    // ListBasedSetLockCouplingRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lcrw(0);
        auto thread_contains_lcrw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += lcrw.contains(work[i]);
            // Add the thread's matches to the global counter
            total_lcrw += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_lcrw);
        EXPECT_EQ(total, total_lcrw);
    }

    // ListBasedSetOptimistic
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_o(0);
        auto thread_contains_o = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += o.contains(work[i]);
            // Add the thread's matches to the global counter
            total_o += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_o);
        EXPECT_EQ(total, total_o);
    }

    // ListBasedSetOptimisticLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_olc(0);
        auto thread_contains_olc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            uint32_t sum = 0;
            for (uint32_t i = range.begin(); i < range.end(); i++)
                sum += olc.contains(work[i]);
            // Add the thread's matches to the global counter
            total_olc += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains_olc);
        EXPECT_EQ(total, total_olc);
    }
}
//---------------------------------------------------------------------------
TEST_F(SynchronizationTest, insert_Dense) {
    /// Setup

    /// Dense, high contention
    // Number of (non-unique) values, which are inserted into the list. Feel free to change it
    static constexpr uint32_t operations = 10e6; // e6
    // Define the domain range of the values.
    static constexpr uint32_t domain = 16;

    work.resize(operations);
    for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

    // Init list-based set. Insert only a small subset into the list-based set.
    for (uint32_t i = 0; i < domain * 0.75; i++) {
        const int64_t value = random() % domain;
        ns.insert(value);
        cl.insert(value);
        crw.insert(value);
        lc.insert(value);
        lcrw.insert(value);
        o.insert(value);
        olc.insert(value);
    }

    /// Real Tests
    tbb::task_scheduler_init taskScheduler;

    // ListBasedSetNoSync
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total(0);


    for (uint32_t i = 0; i < operations; i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
            ns.insert(work[i]);
        }
    }
    uint32_t sum = 0;
    for (uint32_t i = 0; i < operations; i++) {
        sum += ns.contains(work[i]);
    }
    total += sum;
    std::cout << "total: " << total << std::endl;


    // ListBasedSetCoarseLock
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_cl(0);
        auto thread_insert_cl = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    cl.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_cl);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += cl.contains(work[i]);
        }
        total_cl += sum_;
        std::cout << "total_cl: " << total_cl << std::endl;
        EXPECT_EQ(total, total_cl);
    }


    // ListBasedSetCoarseLockRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_crw(0);
        auto thread_insert_crw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    crw.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_crw);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += crw.contains(work[i]);
        }
        total_crw += sum_;
        std::cout << "total_crw: " << total_crw << std::endl;
        EXPECT_EQ(total, total_crw);
    }


    // ListBasedSetLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lc(0);
        auto thread_insert_lc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    lc.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lc);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += lc.contains(work[i]);
        }
        total_lc += sum_;
        std::cout << "total_lc: " << total_lc << std::endl;
        EXPECT_EQ(total, total_lc);
    }


    // ListBasedSetLockCouplingRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lcrw(0);
        auto thread_insert_lcrw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    lcrw.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lcrw);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += lcrw.contains(work[i]);
        }
        total_lcrw += sum_;
        std::cout << "total_lcrw: " << total_lcrw << std::endl;
        EXPECT_EQ(total, total_lcrw);
    }

    // ListBasedSetOptimistic
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_o(0);
        auto thread_insert_o = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    o.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_o);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += o.contains(work[i]);
        }
        total_o += sum_;
        std::cout << "total_o: " << total_o << std::endl;
        EXPECT_EQ(total, total_o);
    }

    // ListBasedSetOptimisticLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_olc(0);
        auto thread_insert_olc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    olc.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_olc);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += olc.contains(work[i]);
        }
        total_olc += sum_;
        std::cout << "total_olc: " << total_olc << std::endl;
        EXPECT_EQ(total, total_olc);
    }
}
//---------------------------------------------------------------------------
TEST_F(SynchronizationTest, insert_Sparse) {
    /// Setup

    /// Sparse, low contention
    // Number of (non-unique) values, which are inserted into the list. Feel free to change it
    static constexpr uint32_t operations = 1024;
    // Define the domain range of the values.
    static constexpr uint32_t domain = 4028;

    work.resize(operations);
    for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

    // Init list-based set. Insert only a small subset into the list-based set.
    for (uint32_t i = 0; i < domain * 0.75; i++) {
        const int64_t value = random() % domain;
        ns.insert(value);
        cl.insert(value);
        crw.insert(value);
        lc.insert(value);
        lcrw.insert(value);
        o.insert(value);
        olc.insert(value);
    }

    /// Real Tests
    tbb::task_scheduler_init taskScheduler;

    // ListBasedSetNoSync
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total(0);


    for (uint32_t i = 0; i < operations; i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
            ns.insert(work[i]);
        }
    }
    uint32_t sum = 0;
    for (uint32_t i = 0; i < operations; i++) {
        sum += ns.contains(work[i]);
    }
    total += sum;
    std::cout << "total: " << total << std::endl;


    // ListBasedSetCoarseLock
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_cl(0);
        auto thread_insert_cl = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    cl.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_cl);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += cl.contains(work[i]);
        }
        total_cl += sum_;
        std::cout << "total_cl: " << total_cl << std::endl;
        EXPECT_EQ(total, total_cl);
    }


    // ListBasedSetCoarseLockRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_crw(0);
        auto thread_insert_crw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    crw.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_crw);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += crw.contains(work[i]);
        }
        total_crw += sum_;
        std::cout << "total_crw: " << total_crw << std::endl;
        EXPECT_EQ(total, total_crw);
    }


    // ListBasedSetLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lc(0);
        auto thread_insert_lc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    lc.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lc);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += lc.contains(work[i]);
        }
        total_lc += sum_;
        std::cout << "total_lc: " << total_lc << std::endl;
        EXPECT_EQ(total, total_lc);
    }


    // ListBasedSetLockCouplingRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lcrw(0);
        auto thread_insert_lcrw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    lcrw.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lcrw);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += lcrw.contains(work[i]);
        }
        total_lcrw += sum_;
        std::cout << "total_lcrw: " << total_lcrw << std::endl;
        EXPECT_EQ(total, total_lcrw);
    }

    // ListBasedSetOptimistic
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_o(0);
        auto thread_insert_o = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    o.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_o);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += o.contains(work[i]);
        }
        total_o += sum_;
        std::cout << "total_o: " << total_o << std::endl;
        EXPECT_EQ(total, total_o);
    }

    // ListBasedSetOptimisticLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_olc(0);
        auto thread_insert_olc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    olc.insert(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_olc);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += olc.contains(work[i]);
        }
        total_olc += sum_;
        std::cout << "total_olc: " << total_olc << std::endl;
        EXPECT_EQ(total, total_olc);
    }
}
//---------------------------------------------------------------------------
TEST_F(SynchronizationTest, remove_Sparse) {
    /// Setup

    /// Sparse, low contention
    // Number of (non-unique) values, which are inserted into the list. Feel free to change it
    static constexpr uint32_t operations = 1024;
    // Define the domain range of the values.
    static constexpr uint32_t domain = 4028;

    work.resize(operations);
    for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

    // Init list-based set. Insert only a small subset into the list-based set.
    for (uint32_t i = 0; i < domain * 0.75; i++) {
        const int64_t value = random() % domain;
        ns.insert(value);
        cl.insert(value);
        crw.insert(value);
        lc.insert(value);
        lcrw.insert(value);
        o.insert(value);
        olc.insert(value);
    }

    /// Real Tests
    tbb::task_scheduler_init taskScheduler;

    // ListBasedSetNoSync
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total(0);


    for (uint32_t i = 0; i < operations; i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
            ns.remove(work[i]);
        }
    }
    uint32_t sum = 0;
    for (uint32_t i = 0; i < operations; i++) {
        sum += ns.contains(work[i]);
    }
    total += sum;
    std::cout << "total: " << total << std::endl;


    // ListBasedSetCoarseLock
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_cl(0);
        auto thread_remove_cl = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    cl.remove(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_remove_cl);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += cl.contains(work[i]);
        }
        total_cl += sum_;
        std::cout << "total_cl: " << total_cl << std::endl;
        EXPECT_EQ(total, total_cl);
    }


    // ListBasedSetCoarseLockRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_crw(0);
        auto thread_remove_crw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    crw.remove(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_remove_crw);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += crw.contains(work[i]);
        }
        total_crw += sum_;
        std::cout << "total_crw: " << total_crw << std::endl;
        EXPECT_EQ(total, total_crw);
    }


    // ListBasedSetLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lc(0);
        auto thread_remove_lc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    lc.remove(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_remove_lc);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += lc.contains(work[i]);
        }
        total_lc += sum_;
        std::cout << "total_lc: " << total_lc << std::endl;
        EXPECT_EQ(total, total_lc);
    }


    // ListBasedSetLockCouplingRW
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_lcrw(0);
        auto thread_remove_lcrw = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    lcrw.remove(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_remove_lcrw);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += lcrw.contains(work[i]);
        }
        total_lcrw += sum_;
        std::cout << "total_lcrw: " << total_lcrw << std::endl;
        EXPECT_EQ(total, total_lcrw);
    }

    // ListBasedSetOptimistic
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_o(0);
        auto thread_remove_o = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    o.remove(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_remove_o);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += o.contains(work[i]);
        }
        total_o += sum_;
        std::cout << "total_o: " << total_o << std::endl;
        EXPECT_EQ(total, total_o);
    }

    // ListBasedSetOptimisticLockCoupling
    {
        taskScheduler.terminate();
        taskScheduler.initialize(threads);
        atomic<uint32_t> total_olc(0);
        auto thread_remove_olc = [&](const tbb::blocked_range<uint32_t> &range) {
            // Count the number of keys contained in the list-based set
            for (uint32_t i = range.begin(); i < range.end(); i++) {
                if (i % 10 < 6) { // Decide the executed action for this key
                    olc.remove(work[i]);
                }
            }
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_remove_olc);
        // Add the thread's matches to the global counter
        uint32_t sum_ = 0;
        for (uint32_t i = 0; i < operations; i++) {
            sum_ += olc.contains(work[i]);
        }
        total_olc += sum_;
        std::cout << "total_olc: " << total_olc << std::endl;
        EXPECT_EQ(total, total_olc);
    }
}
//---------------------------------------------------------------------------
TEST_F(SynchronizationTest, mixed_Dense) {
  /// Setup

  /// Dense, high contention
  // Number of (non-unique) values, which are inserted into the list. Feel free to change it
  static constexpr uint32_t operations = 10e6;
  // Define the domain range of the values.
  static constexpr uint32_t domain = 16;

  work.resize(operations);
  for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

  // Init list-based set. Insert only a small subset into the list-based set.
  for (uint32_t i = 0; i < domain * 0.75; i++) {
    const int64_t value = random() % domain;
    ns.insert(value);
    cl.insert(value);
    crw.insert(value);
    lc.insert(value);
    lcrw.insert(value);
    o.insert(value);
    olc.insert(value);
  }

  /// Real Tests
  tbb::task_scheduler_init taskScheduler;

  // ListBasedSetNoSync
  taskScheduler.terminate();
  taskScheduler.initialize(threads);
  atomic<uint32_t> total(0);

  for (uint32_t i = 0; i < operations; i++) {
    if (i % 10 < 6) { // Decide the executed action for this key
      ns.contains(work[i]);
    } else if (i % 10 < 8) {
      ns.insert(work[i]);
    } else {
      ns.remove(work[i]);
    }
  }

  uint32_t sum = 0;
  for (uint32_t i = 0; i < operations; i++) {
    sum += ns.contains(work[i]);
  }
  total += sum;
  std::cout << "total: " << total << std::endl;


  // ListBasedSetCoarseLock
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_cl(0);
    auto thread_insert_cl = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          cl.contains(work[i]);
        } else if (i % 10 < 8) {
          cl.insert(work[i]);
        } else {
          cl.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_cl);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += cl.contains(work[i]);
    }
    total_cl += sum_;
    std::cout << "total_cl: " << total_cl << std::endl;
//    EXPECT_EQ(total, total_cl);
  }


  // ListBasedSetCoarseLockRW
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_crw(0);
    auto thread_insert_crw = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          crw.contains(work[i]);
        } else if (i % 10 < 8) {
          crw.insert(work[i]);
        } else {
          crw.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_crw);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += crw.contains(work[i]);
    }
    total_crw += sum_;
    std::cout << "total_crw: " << total_crw << std::endl;
//    EXPECT_EQ(total, total_crw);
  }


  // ListBasedSetLockCoupling
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_lc(0);
    auto thread_insert_lc = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          lc.contains(work[i]);
        } else if (i % 10 < 8) {
          lc.insert(work[i]);
        } else {
          lc.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lc);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += lc.contains(work[i]);
    }
    total_lc += sum_;
    std::cout << "total_lc: " << total_lc << std::endl;
//    EXPECT_EQ(total, total_lc);
  }


  // ListBasedSetLockCouplingRW
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_lcrw(0);
    auto thread_insert_lcrw = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          lcrw.contains(work[i]);
        } else if (i % 10 < 8) {
          lcrw.insert(work[i]);
        } else {
          lcrw.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lcrw);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += lcrw.contains(work[i]);
    }
    total_lcrw += sum_;
    std::cout << "total_lcrw: " << total_lcrw << std::endl;
//    EXPECT_EQ(total, total_lcrw);
  }

  // ListBasedSetOptimistic
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_o(0);
    auto thread_insert_o = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          o.contains(work[i]);
        } else if (i % 10 < 8) {
          o.insert(work[i]);
        } else {
          o.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_o);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += o.contains(work[i]);
    }
    total_o += sum_;
    std::cout << "total_o: " << total_o << std::endl;
//    EXPECT_EQ(total, total_o);
  }


  // ListBasedSetOptimisticLockCoupling
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_olc(0);
    auto thread_insert_olc = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          olc.contains(work[i]);
        } else if (i % 10 < 8) {
          olc.insert(work[i]);
        } else {
          olc.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_olc);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += olc.contains(work[i]);
    }
    total_olc += sum_;
    std::cout << "total_olc: " << total_olc << std::endl;
//    EXPECT_EQ(total, total_olc);
  }
}
//---------------------------------------------------------------------------
TEST_F(SynchronizationTest, mixed_Sparse) {
  /// Setup

  /// Sparse, low contention
  // Number of (non-unique) values, which are inserted into the list. Feel free to change it
  static constexpr uint32_t operations = 1024;
  // Define the domain range of the values.
  static constexpr uint32_t domain = 4028;

  work.resize(operations);
  for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

  // Init list-based set. Insert only a small subset into the list-based set.
  for (uint32_t i = 0; i < domain * 0.75; i++) {
    const int64_t value = random() % domain;
    ns.insert(value);
    cl.insert(value);
    crw.insert(value);
    lc.insert(value);
    lcrw.insert(value);
    o.insert(value);
    olc.insert(value);
  }

  /// Real Tests
  tbb::task_scheduler_init taskScheduler;

  // ListBasedSetNoSync
  taskScheduler.terminate();
  taskScheduler.initialize(threads);
  atomic<uint32_t> total(0);

  for (uint32_t i = 0; i < operations; i++) {
    if (i % 10 < 6) { // Decide the executed action for this key
      ns.contains(work[i]);
    } else if (i % 10 < 8) {
      ns.insert(work[i]);
    } else {
      ns.remove(work[i]);
    }
  }

  uint32_t sum = 0;
  for (uint32_t i = 0; i < operations; i++) {
    sum += ns.contains(work[i]);
  }
  total += sum;
  std::cout << "total: " << total << std::endl;


  // ListBasedSetCoarseLock
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_cl(0);
    auto thread_insert_cl = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          cl.contains(work[i]);
        } else if (i % 10 < 8) {
          cl.insert(work[i]);
        } else {
          cl.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_cl);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += cl.contains(work[i]);
    }
    total_cl += sum_;
    std::cout << "total_cl: " << total_cl << std::endl;
//    EXPECT_EQ(total, total_cl);
  }


  // ListBasedSetCoarseLockRW
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_crw(0);
    auto thread_insert_crw = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          crw.contains(work[i]);
        } else if (i % 10 < 8) {
          crw.insert(work[i]);
        } else {
          crw.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_crw);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += crw.contains(work[i]);
    }
    total_crw += sum_;
    std::cout << "total_crw: " << total_crw << std::endl;
//    EXPECT_EQ(total, total_crw);
  }


  // ListBasedSetLockCoupling
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_lc(0);
    auto thread_insert_lc = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          lc.contains(work[i]);
        } else if (i % 10 < 8) {
          lc.insert(work[i]);
        } else {
          lc.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lc);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += lc.contains(work[i]);
    }
    total_lc += sum_;
    std::cout << "total_lc: " << total_lc << std::endl;
//    EXPECT_EQ(total, total_lc);
  }


  // ListBasedSetLockCouplingRW
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_lcrw(0);
    auto thread_insert_lcrw = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          lcrw.contains(work[i]);
        } else if (i % 10 < 8) {
          lcrw.insert(work[i]);
        } else {
          lcrw.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_lcrw);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += lcrw.contains(work[i]);
    }
    total_lcrw += sum_;
    std::cout << "total_lcrw: " << total_lcrw << std::endl;
//    EXPECT_EQ(total, total_lcrw);
  }

  // ListBasedSetOptimistic
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_o(0);
    auto thread_insert_o = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
          o.contains(work[i]);
        } else if (i % 10 < 8) {
          o.insert(work[i]);
        } else {
          o.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_o);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += o.contains(work[i]);
    }
    total_o += sum_;
    std::cout << "total_o: " << total_o << std::endl;
//    EXPECT_EQ(total, total_o);
  }


  // ListBasedSetOptimisticLockCoupling
  {
    taskScheduler.terminate();
    taskScheduler.initialize(threads);
    atomic<uint32_t> total_olc(0);
    auto thread_insert_olc = [&](const tbb::blocked_range<uint32_t> &range) {
      // Count the number of keys contained in the list-based set
      for (uint32_t i = range.begin(); i < range.end(); i++) {
        if (i % 10 < 6) { // Decide the executed action for this key
//          olc.contains(work[i]);
        } else if (i % 10 < 8) {
          olc.insert(work[i]);
        } else {
          olc.remove(work[i]);
        }
      }
    };
    // Run thread_contains in parallel
    tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_insert_olc);
    // Add the thread's matches to the global counter
    uint32_t sum_ = 0;
    for (uint32_t i = 0; i < operations; i++) {
      sum_ += olc.contains(work[i]);
    }
    total_olc += sum_;
    std::cout << "total_olc: " << total_olc << std::endl;
//    EXPECT_EQ(total, total_olc);
  }
}
//---------------------------------------------------------------------------
