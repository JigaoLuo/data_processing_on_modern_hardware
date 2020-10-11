#include "access_patterns.h"

#include <stdint.h>
#include <string.h>
#include <stdio.h>

static inline void
getcyclecount(uint64_t* cycles) {
  __asm volatile(
  "cpuid # force all prev. instr. to complete\n\t"
  "rdtsc # TSC → edx:eax \n\t"
  "movl %%edx, 4(%0) # store edx\n\t"
  "movl %%eax, 0(%0) # store eax\n\t"
  : :"r"(cycles) : "eax", "ebx", "ecx", "edx");
}

int compare( const void* a , const void* b ) {
  const uint64_t ai = *(uint64_t*)a;
  const uint64_t bi = *(uint64_t*)b;
  if( ai < bi ) {
    return -1;
  } else if( ai > bi ) {
    return 1;
  } else {
    return 0;
  }
}

int main(int argc, char *argv[]) {
  if (argc < 2 + 1 || argc > 3 + 1) {
    return 1;
  }

  static_assert(sizeof(struct l) == 8, "8 bytes pro element");

  int RUNS = 3;
  assert(RUNS % 2 == 1);

  uint64_t cycles_runs[RUNS];
  memset(cycles_runs, 0, RUNS * sizeof(uint64_t));

  // 0 := random | 1 := seqyuential | 2 := reverse sequential
  char *argv1 = argv[1];
  const int access_pattern = atoi(argv1);
  if (access_pattern < 0 || access_pattern > 2) {
    return 2;
  }

  char *argv2 = argv[2];
  const int working_set_exponent = atoi(argv2);
  assert(working_set_exponent >= 8 && working_set_exponent <= 30);
  if (working_set_exponent < 8 && working_set_exponent > 30) {
    return 3;
  }
//  if (working_set_exponent >= 24) {
//    RUNS = 101;
//  }
  const int working_set_size = 1 << working_set_exponent;
  const int array_size = working_set_size / 8;

//  struct l array[array_size];
  struct l *array = (struct l *)malloc(array_size * sizeof(struct l));

  if (access_pattern == 0) {
    Init_Random(array, array_size);
  } else {
    char *argv3 = argv[3];
    const int stride_exponent = atoi(argv3);
    assert(stride_exponent >= 1 && stride_exponent < working_set_exponent);
    if (stride_exponent < 3 && stride_exponent >= working_set_exponent) {
      return 4;
    }
    const int stride_size = 1 << stride_exponent;
    const int stride_offset = stride_size / sizeof(struct l);
    const int inner_iter = array_size / stride_offset;
    const int outer_iter = array_size / inner_iter;

    if (access_pattern == 1) {
      Init_Sequential(array, inner_iter, outer_iter, stride_offset);
    } else {
      Init_Rev_Sequential(array + array_size - 1, inner_iter, outer_iter, stride_offset);
    }
  }

  volatile uint64_t start = 0;
  volatile uint64_t end = 0;
  for (int i = 0; i < RUNS; i++) {
    getcyclecount((uint64_t*) &start);
    Access(array, array_size);
    getcyclecount((uint64_t*) &end);
    cycles_runs[i] = end - start;
  }

  qsort(cycles_runs, RUNS, sizeof(uint64_t), compare);

  if (access_pattern == 0) {
    printf("%d, %lu\n", working_set_size, cycles_runs[RUNS / 2]);
  } else {
    char *argv3 = argv[3];
    const int stride_exponent = atoi(argv3);
    printf("%d, %d, %lu\n", working_set_size, (1 << stride_exponent), cycles_runs[RUNS / 2]);
  }
  free(array);
  return 0;
}

