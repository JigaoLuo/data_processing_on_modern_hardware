/**
 * @file
 *
 * Based on code from Viktor Leis (Friedrich Schiller University Jena)
 *
 * @authors: Jana Giceva <jana.giceva@in.tum.de>, Alexander Beischl
 * <beischl@in.tum.de>
 */

#include "aggregation.hpp"
#include "perfEvent.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <vector>
#include <functional>

using namespace std;

int main() {
  static constexpr int32_t inCount = 1024ull * 1024 * 128;

  auto in8 = reinterpret_cast<int8_t *>(malloc_huge(inCount * sizeof(int8_t)));
  auto in64 = reinterpret_cast<int64_t *>(malloc_huge(inCount * sizeof(int64_t)));

  for (int32_t i = 0; i < inCount; i++) {
    in8[i] = random() % 100;
    in64[i] = random() % 100;
  }

  // ToDo make sure the chunk size is a multiple of the SIMD lane so you don't need to handle not full lanes
  static constexpr unsigned chunkSize = 1024 * 1024;

  const std::vector<std::function<unsigned(int8_t*, unsigned, int8_t)>> count8_functions{count8, count8BrFree, count8SIMD};
  const std::vector<std::string> count8_function_names{"        count8", "  count8BrFree", "    count8SIMD"};
  const std::vector<std::function<unsigned(int64_t*, unsigned, int64_t)>> count64_functions{count64, count64BrFree, count64SIMD};
  const std::vector<std::string> count64_function_names{"        count64", "  count64BrFree", "    count64SIMD"};

  BenchmarkParameters params;
  for (auto sel : {1, 10, 50, 90, 99}) {
    for (size_t it = 0; it < count8_functions.size(); it++) {
      params.setParam("name", count8_function_names[it]);
      params.setParam("selectivity", sel);
      PerfEventBlock b(1, params);

      static constexpr unsigned chunkCount = (inCount * sizeof(uint8_t)) / chunkSize;
      for (unsigned i = 0; i < chunkCount; i++)
        assert(count8_functions[it](in8, inCount / chunkCount, sel));
    }
  }

  for (auto sel : {1, 10, 50, 90, 99}) {
    for (size_t it = 0; it < count64_functions.size(); it++) {
      // Uncomment this code block for profiling
      params.setParam("name", count64_function_names[it]);
      params.setParam("selectivity", sel);
      PerfEventBlock b(1, params);

      static constexpr unsigned chunkCount = (inCount * sizeof(uint64_t)) / chunkSize;
      for (unsigned i = 0; i < chunkCount; i++)
        assert(count64_functions[it](in64, inCount / chunkCount, sel));
    }
  }
  
  cout << endl;
  return 0;
}
