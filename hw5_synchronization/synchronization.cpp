#include "perfEvent.hpp"
#include "ListBasedSetNoSync.hpp"
#include "ListBasedSetSync.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

// How to install:
// https://askubuntu.com/questions/1170054/install-newest-tbb-thread-building-blocks-on-ubuntu-18-04
#include <tbb/tbb.h>

using namespace std;

// Use this enum for our experiments
enum class Synchronization {
  coarse,
  coarseRW,
  coupling,
  couplingRW,
  optimistic,
  optimisticCoupling
};

int main(int argc, char **argv) {
  // Number of (non-unique) values, which are inserted into the list. Feel free to change it

  /// Sparse, low contention
//  // Number of (non-unique) values, which are inserted into the list. Feel free to change it
//  static constexpr uint32_t operations = 10e6;
//  // Define the domain range of the values.
//  static constexpr uint32_t domain = 10e6;

  /// Dense, high contention
  // Number of (non-unique) values, which are inserted into the list. Feel free to change it
  static constexpr uint32_t operations = 10e6;
  // Define the domain range of the values. ToDo: Play around with this parameter.
  static constexpr uint32_t domain = 16;

  // Vector containing the number of threads for the experiments
  // ToDo: adapt for used machine, test more software threads than available hardware threads as well
  const vector<int> threads = {1, 2, 5, 10, 15, 20, 30, 40};

  // Vector containing the dataset
  vector<uint32_t> work(operations);
  for (uint32_t i = 0; i < operations; i++) work[i] = random() % domain; // Create elements & map them to the domain

  // Initialize the list-based set
  ListBasedSetNoSync<int64_t> ns;
  ListBasedSetCoarseLock<int64_t, tbb::mutex> cl;
  ListBasedSetCoarseLockRW<int64_t, tbb::spin_rw_mutex> crw;
  ListBasedSetLockCoupling<int64_t, tbb::mutex> lc;
  ListBasedSetLockCouplingRW<int64_t, tbb::spin_rw_mutex> lcrw;
  ListBasedSetOptimistic<int64_t, tbb::mutex> o;
  ListBasedSetOptimisticLockCoupling<int64_t, tbb::spin_rw_mutex> olc;

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

  std::vector<ListBasedSetSync<int64_t>*> list_vec;
  list_vec.emplace_back(&cl);
  list_vec.emplace_back(&crw);
  list_vec.emplace_back(&lc);
  list_vec.emplace_back(&lcrw);
  list_vec.emplace_back(&o);
  list_vec.emplace_back(&olc);

  // TODO(jigao): dense and sparse

  /*--------------------------- Read-only workload ---------------------------*/

  // Task scheduler from TBB, used to distribute the tasks to the threads
  tbb::task_scheduler_init taskScheduler;

  // Multi-threaded baseline
  for (int t : threads) {
    taskScheduler.terminate();
    taskScheduler.initialize(t);

    // Thread-safe global counter for the number of keys contained in the set
    atomic<uint32_t> total(0);

    // Block for profiling
    BenchmarkParameters params;
    params.setParam("workload", "contains");
    params.setParam("approach", "nosync");
    params.setParam("threads", t);
    params.setParam("domain", domain);
    PerfEventBlock b(1, params);

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
  }

  // Your implementation
  for (auto& list : list_vec) {
    for (int t : threads) {
      taskScheduler.terminate();
      taskScheduler.initialize(t);

      // Thread-safe global counter for the number of keys contained in the set
      atomic<uint32_t> total(0);

      // Block for profiling
      BenchmarkParameters params;
      params.setParam("workload", "contains");
      params.setParam("approach", list->get_name());
      params.setParam("threads", t);
      params.setParam("domain", domain);
      // params.setParam("synchronization", locking); use the enum here
      PerfEventBlock b(1, params);

      auto thread_contains = [&](const tbb::blocked_range<uint32_t> &range) {
        // Count the number of keys contained in the list-based set
        uint32_t sum = 0;
        for (uint32_t i = range.begin(); i < range.end(); i++)
          sum += list->contains(work[i]);
        // Add the thread's matches to the global counter
        total += sum;
      };

      // Run thread_contains in parallel
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_contains);
    }
  }


  /*----------------------------- Mixed workload -----------------------------*/

  { // Single-threaded baseline

    // Thread-safe global counter for the number of keys contained in the set
    atomic<uint32_t> total(0);

    // Block for profiling
    BenchmarkParameters params;
    params.setParam("workload", "mix");
    params.setParam("approach", "nosync");
    params.setParam("threads", "1");
    params.setParam("domain", domain);
    PerfEventBlock b(1, params);

    uint32_t sum = 0;

    for (uint32_t i = 0; i < operations; i++) {
      if (i % 10 < 6) { // Decide the executed action for this key
        sum += ns.contains(work[i]);
      } else if (i % 10 < 8) {
        ns.insert(work[i]);
      } else {
        ns.remove(work[i]);
      }
    }

    total += sum;
  }

  // Your implementation
  for (auto& list : list_vec) {
    for (int t : threads) {
      taskScheduler.terminate();
      taskScheduler.initialize(t);

      {
        // Thread-safe global counter for the number of keys contained in the set
        atomic<uint32_t> total(0);

        // Block for profiling
        BenchmarkParameters params;
        params.setParam("workload", "mix");
        params.setParam("approach", list->get_name());
        params.setParam("threads", t);
        params.setParam("domain", domain);
        PerfEventBlock b(1, params);

        auto thread_mix = [&](const tbb::blocked_range<uint32_t> &range) {
          uint32_t sum = 0;
          // Count the number of keys contained in the list-based set
          for (uint32_t i = range.begin(); i < range.end(); i++) {
            if (i % 10 < 6) { // Decide the executed action for this key
              sum += list->contains(work[i]);
            } else if (i % 10 < 8) {
              list->insert(work[i]);
            } else {
              list->remove(work[i]);
            }
          }
          total += sum;
        };
        // Run thread_contains in parallel
        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, operations), thread_mix);
      }
    }
  }

  return 0;
}
