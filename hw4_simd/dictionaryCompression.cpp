/**
 * @file
 *
 * Based on code from Viktor Leis (Friedrich Schiller University Jena)
 *
 * @authors: Jana Giceva <jana.giceva@in.tum.de>, Alexander Beischl
 * <beischl@in.tum.de>
 */

#include "dictionaryCompression.hpp"
#include "perfEvent.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <ostream>
#include <vector>
#include <array>
#include <functional>

using namespace std;

int main() {
  static constexpr int32_t inCountBits = 23;
  static constexpr int32_t inCount = 1ull << inCountBits;
  unsigned repeat = 100;

  {
    // dictionary compression
    auto in8 = reinterpret_cast<uint8_t *>(malloc_huge(inCount * sizeof(uint8_t)));
    auto out32 = reinterpret_cast<int32_t *>(malloc_huge(inCount * 2 * sizeof(int32_t)));

    for (int32_t i = 0; i < inCount; i++)
      in8[i] = random();

    static constexpr unsigned chunkSize = 6 * 1024; // ToDo make sure the chunk size is a multiple of the SIMD lane so you don't need to handle not full lanes

    const std::vector<std::function<void(uint8_t*, uint32_t, int32_t*)>> dictDecompress_functions{dictDecompress, dictDecompress_BYTE, dictDecompress8, dictDecompressGather, dictDecompressPermute};
    const std::vector<std::string> dictDecompress_names{"        dictDecompress", "   dictDecompress_BYTE", "       dictDecompress8", "  dictDecompressGather", "  dictDecompressPermute"};

    BenchmarkParameters params;
    for (size_t it = 0; it < dictDecompress_functions.size(); it++) {
      {
        params.setParam("name", dictDecompress_names[it]);
        PerfEventBlock b(1, params);

        static constexpr unsigned chunk = (inCount * sizeof(uint8_t) + 2 * inCount * sizeof(uint32_t)) / chunkSize;
        for (unsigned r = 0; r < repeat; r++) {
          for (unsigned i = 0; i < chunk; i++)
            dictDecompress_functions[it](in8, inCount / chunk, out32);
        }
      }
    }

  }
  cout << endl;

  return 0;
}
