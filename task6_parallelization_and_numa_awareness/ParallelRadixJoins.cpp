//
// Created by Alexander Beischl on 2020-05-19.
//
#include "ParallelRadixJoins.hpp"
#include "perfEvent.hpp"
#include <cassert>
#include <ctime>
#include <iostream>
#include <sstream>


int main(int argc, char *argv[]) {
  constexpr uint64_t domain = 16;
  constexpr uint64_t size_r = 1 << 23;  // 8Mi  * 16 Byte = 128 MiB
  constexpr uint64_t size_s = 1 << 26;  // 64Mi * 16 Byte = 1024 MiB | 1GiB
  // Change the partitioning bits to vary the granularity
  constexpr uint8_t partitioningBits = 14;
  std::srand(std::time(nullptr));

  relation r(size_r);
  relation s(size_s);

  // Fill r
  for (size_t i = 0; i < size_r; i++) {
    tuple_t t;
    t.key = rand() % (1 << domain);
    r[i] = t;
  }

  // Fill s
  for (size_t i = 0; i < size_s; i++) {
    tuple_t t;
    t.key = rand() % (1 << domain);
    s[i] = t;
  }

  // Number of repetitions for the experiments
  unsigned repeat = 10;
  uint64_t valCnt = 0; // Validation counter
  uint64_t matches = 0;
  bool printHeader = true;
  // Feel free th change the threads set
  const std::vector<uint8_t> threads = {1, 2, 4, 8, 16, 32, 64};

  // Warm-up the caches
  auto testCnt = radix_join(r, s, Partitioning::naive, partitioningBits);

  // Baseline and to check correctness
  {
    BenchmarkParameters params;
    params.setParam("name", "baseline");
    PerfEventBlock b(size_r, params, printHeader);
    printHeader = false;

    for (unsigned i = 0; i < repeat; i++)
      valCnt += radix_join(r, s, Partitioning::naive, partitioningBits);
  }
  assert(valCnt == repeat*testCnt);


  for (auto t : threads) {
    {
      BenchmarkParameters params;
      params.setParam("name", "Parallel");
      params.setParam("threads", t);
      PerfEventBlock b(size_r, params, printHeader);

      for (unsigned i = 0; i < repeat; i++)
        matches += parallel_radix_join(r, s, partitioningBits, t);
    }
    assert(valCnt == matches);
    matches = 0;
  }

  for (auto t : threads) {
    {
      BenchmarkParameters params;
      params.setParam("name", "NUMA-Aware");
      params.setParam("threads", t);
      PerfEventBlock b(size_r, params, printHeader);

      for (unsigned i = 0; i < repeat; i++)
        matches += numa_radix_join(r, s, partitioningBits, t);
    }
    assert(valCnt == matches);
    matches = 0;
  }

  return 0;
}
