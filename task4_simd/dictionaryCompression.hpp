#pragma once

#include "Util.hpp"

#include <algorithm>
#include <cassert>
#include <immintrin.h>
#include <vector>
#include <array>

using namespace std;

// Decompress 4 bit to 32 bit using the following dictionary:
constexpr std::array<int32_t, 16> nibble_dict = {100, 101, 102, 103,
                                                 200, 201, 202, 203,
                                                 300, 301, 302, 303,
                                                 400, 401, 402, 403};

constexpr std::array<int64_t, 256> fill_byte_dict() {
  std::array<int64_t, 256> byte_dict{0};
  for (size_t i = 0; i < 256; i++) {
    // Non-Trivial: Assumption: Little-Endian
    byte_dict[i] = (static_cast<int64_t>(nibble_dict[i & 0b1111]) << 32) + nibble_dict[(i >> 4) & 0b1111];
  }
  return byte_dict;
}

// Assumption: Decompress two elements at a time
// Decompress 8 bit to 64 bit using the following dictionary:
constexpr std::array<int64_t, 256> byte_dict = fill_byte_dict();

void dictDecompress(uint8_t *in, uint32_t inCount, int32_t *out) {
  // Load 1 Byte -> Write 8 Bytes
  for (uint32_t i = 0; i < inCount; i++) {
    out[(i * 2) + 0] = nibble_dict[(in[i] >> 4) & 0b1111];
    out[(i * 2) + 1] = nibble_dict[in[i] & 0b1111];
  }
}

void dictDecompress_BYTE(uint8_t *in, uint32_t inCount, int32_t *out) {
  assert((inCount & 1) == 0);  // Assumption: inCount is even
  // Load 1 Byte -> Write 8 Bytes
  int64_t *out_64 = reinterpret_cast<int64_t*>(out);
  for (uint32_t i = 0; i < inCount; i++) {
    out_64[i] = byte_dict[in[i]];
  }
}

// Assumption: Decompress two elements at a time
void dictDecompress8(uint8_t *in, uint32_t inCount, int32_t *out) {
  assert((inCount & 1) == 0);  // Assumption: inCount is even

  // coordination variables
  int64_t *out_64 = reinterpret_cast<int64_t*>(out);
  uint32_t cursor = 0;

  // Load 8 Byte -> Write 64 Bytes
  while (cursor <= inCount - 8) {
    out_64[cursor] = byte_dict[in[cursor]];
    out_64[cursor + 1] = byte_dict[in[cursor + 1]];
    out_64[cursor + 2] = byte_dict[in[cursor + 2]];
    out_64[cursor + 3] = byte_dict[in[cursor + 3]];
    out_64[cursor + 4] = byte_dict[in[cursor + 4]];
    out_64[cursor + 5] = byte_dict[in[cursor + 5]];
    out_64[cursor + 6] = byte_dict[in[cursor + 6]];
    out_64[cursor + 7] = byte_dict[in[cursor + 7]];
    cursor += 8;
  }

  // scalar count remaining values
  for (; cursor < inCount; cursor++) {
    out_64[cursor] = byte_dict[in[cursor]];
  }
}

void dictDecompressGather(uint8_t *in, uint32_t inCount, int32_t *out) {
  // coordination variables
  int64_t *out_64 = reinterpret_cast<int64_t*>(out);
  unsigned int cursor = 0;
  __m512i indexes;
  __m512i values;

  // parallel count full blocks
  while (cursor <= inCount - 8) {
    // Here can only set (or setr), load is wrong, since we are explicitly converting uint8_t to int64_t
    indexes = _mm512_set_epi64(in[cursor + 7], in[cursor + 6], in[cursor + 5], in[cursor + 4],
                               in[cursor + 3], in[cursor + 2], in[cursor + 1], in[cursor + 0]);
    values = _mm512_i64gather_epi64(indexes, reinterpret_cast<const void*>(&byte_dict), 8);
    _mm512_store_epi64(out_64 + cursor, values);
    cursor += 8;
  }

  //  scalar count remaining values
  for (; cursor < inCount; cursor++) {
    out_64[cursor] = byte_dict[in[cursor]];
  }
}

void dictDecompressPermute(uint8_t *in, uint32_t inCount, int32_t *out) {
   const __m512i SIMD_dict = _mm512_set_epi32(nibble_dict[15], nibble_dict[14], nibble_dict[13], nibble_dict[12],
                                              nibble_dict[11], nibble_dict[10], nibble_dict[9],  nibble_dict[8],
                                              nibble_dict[7],  nibble_dict[6],  nibble_dict[5],  nibble_dict[4],
                                              nibble_dict[3],  nibble_dict[2],  nibble_dict[1],  nibble_dict[0]);

   // coordination variables
   unsigned int cursor = 0;
   __m512i indexes;
   __m512i values;

   // parallel count full blocks
   while (cursor <= inCount - 8) {
     // Here can only set (or setr), load is wrong, since we are explicitly converting a nibble (4 Bits) to int32_t
     // The precomputed table is helpless for this case xD
     indexes = _mm512_set_epi32(in[cursor + 7] & 0b1111, (in[cursor + 7] >> 4), in[cursor + 6] & 0b1111, (in[cursor + 6] >> 4),
                                in[cursor + 5] & 0b1111, (in[cursor + 5] >> 4), in[cursor + 4] & 0b1111, (in[cursor + 4] >> 4),
                                in[cursor + 3] & 0b1111, (in[cursor + 3] >> 4), in[cursor + 2] & 0b1111, (in[cursor + 2] >> 4),
                                in[cursor + 1] & 0b1111, (in[cursor + 1] >> 4), in[cursor + 0] & 0b1111, (in[cursor + 0] >> 4));
     values = _mm512_permutexvar_epi32(indexes, SIMD_dict);
     _mm512_store_epi64(out + cursor * 2, values);
     cursor += 8;
   }

   //  scalar count remaining values
   int64_t *out_64 = reinterpret_cast<int64_t*>(out);
   for (; cursor < inCount; cursor++) {
     out_64[cursor] = byte_dict[in[cursor]];
   }
}