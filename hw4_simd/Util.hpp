#pragma once

#include <cstring>
#include <immintrin.h>

void *malloc_huge(size_t size);

void _mm512i_print_epi64 (__m512i v);

void _mm512i_print_epi32 (__m512i v);
