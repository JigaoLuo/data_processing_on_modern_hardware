#pragma once

#include "Util.hpp"

#include <algorithm>
#include <cassert>
#include <immintrin.h>
#include <iostream>
#include <ostream>
#include <vector>

#include "perfEvent.hpp"

// Compute the number of entries that are less than x
unsigned count8(int8_t *in, unsigned inCount, int8_t x) {
  __asm volatile(""); // make sure this is not optimized away when called multiple times
  unsigned count = 0;
  for (unsigned i = 0; i < inCount; i++)
    if (in[i] < x)
      count++;
  return count;
}

// Compute the number of entries that are less than x
unsigned count64(int64_t *in, unsigned inCount, int64_t x) {
  __asm volatile(""); // make sure this is not optimized away when called multiple times
  unsigned count = 0;
  for (unsigned i = 0; i < inCount; i++)
    if (in[i] < x)
      count++;
  return count;
}

// Compute the number of entries that are less than x without branching
unsigned count8BrFree(int8_t *in, unsigned inCount, int8_t x) {
  __asm volatile(""); // make sure this is not optimized away when called multiple times
  unsigned count = 0;
  for (unsigned i = 0; i < inCount; i++) count += (in[i] < x);
  return count;
}

// Compute the number of entries that are less than x without branching
unsigned count64BrFree(int64_t *in, unsigned inCount, int64_t x) {
  __asm volatile(""); // make sure this is not optimized away when called multiple times
  unsigned count = 0;
  for (unsigned i = 0; i < inCount; i++) count += (in[i] < x);
  return count;
}

unsigned count8SIMD(int8_t *in, unsigned inCount, int8_t x) {
  __asm volatile("");
  assert(reinterpret_cast<std::uintptr_t>(in) % 64 == 0 && "Memory alignment is 64 bytes!");
  // coordination variables
  unsigned int cursor = 0;

  // aggregation variables
  __m512i constants = _mm512_set1_epi8(x);
  __m512i values;
  __mmask64 mask;
  unsigned count = 0;

  // parallel count full blocks
  while (cursor <= inCount - 64) {
    values = _mm512_load_si512(in + cursor);
    mask = _mm512_cmplt_epi8_mask(values, constants);
    count += _mm_popcnt_u64(mask);
    cursor += 64;
  }
  // scalar count remaining values
  for (; cursor < inCount; cursor++) {
    count += (in[cursor] < x);
  }
  return count;
}

// Compute the number of entries that are less than x using SIMD instructions
unsigned count64SIMD(int64_t *in, unsigned inCount, int64_t x) {
  __asm volatile("");
  assert(reinterpret_cast<std::uintptr_t>(in) % 64 == 0 && "Memory alignment is 64 bytes!");
  // coordination variables
  unsigned int cursor = 0;

  // aggregation variables
  __m512i constants = _mm512_set1_epi64(x);
  __m512i values;
  __mmask64 mask;
  unsigned count = 0;

  // parallel count full blocks
  while (cursor <= inCount - 8) {
    values = _mm512_load_si512(in + cursor);
    mask = _mm512_cmplt_epi64_mask(values, constants);
    count += __builtin_popcount(mask);
    cursor += 8;
  }
  // scalar count remaining values
  for (; cursor < inCount; cursor++) {
    count += (in[cursor] < x);
  }
  return count;
}