#include <assert.h>
#include <stdlib.h>

#pragma once

#ifndef INCLUDE_ACCESS_PATTERNS_H_
#define INCLUDE_ACCESS_PATTERNS_H_
// ---------------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------
struct l {
  struct l *n;
};

int ValueInArray(int val, int *array, int array_size) {
  for(int i = 0; i < array_size; i++) {
    if(array[i] == val)
      return 1;
  }
  return 0;
}

// Not trivial shuffle, but to generate a single graph containing only one circle
void Shuffle(int *permuted_index, int array_size) {
//  int processed_index[array_size - 1];  // without the 0, since 0 is always the last
  int *processed_index = (int *)malloc((array_size - 1) * sizeof(int));

  int current_index = 0;
  assert(RAND_MAX > array_size);
  // (array_size - 1) times exchange
  for (int i = 0; i < array_size - 1; i++) {
    int random_number = rand();
    int next_index = random_number % array_size;
    while (next_index == 0 || ValueInArray(next_index, processed_index, i) == 1) {
      next_index = rand() % array_size;
    }
    processed_index[i] = next_index;
    int tmp = permuted_index[next_index];
    assert(permuted_index[current_index] == 0);
    permuted_index[next_index] = permuted_index[current_index];
    permuted_index[current_index] = tmp;

    current_index = next_index;
  }
  free(processed_index);
}

void Init_Random(struct l *array, int array_size) {
//  int permuted_index[array_size];
  int *permuted_index = (int *)malloc(array_size * sizeof(int));

  for (int i = 0; i < array_size; i++) {
    permuted_index[i] = i;
  }
  Shuffle(permuted_index, array_size);

  int next_offset = permuted_index[0];
  struct l *current = array;
  for (int i = 0; i < array_size; i++) {
    current->n = array + next_offset;
    current = array + next_offset;
    if (i == array_size) {
      assert(next_offset == 0);
      break;
    }
    next_offset = permuted_index[next_offset];
  }
  free(permuted_index);
}

void Init_Sequential(struct l *array, int inner_iter, int outer_iter, int stride_offset) {
  for (int o_iter = 0; o_iter < outer_iter; o_iter++) {
    int index = o_iter;
    for (int i_iter = 0; i_iter < inner_iter - 1; i_iter++, index += stride_offset) {
      (array + index)->n = array + index + stride_offset;
    }
    (array + index)->n = array + o_iter + 1;
  }
  (array + inner_iter * outer_iter - 1)->n = array;
}

void Init_Rev_Sequential(struct l *array_last, int inner_iter, int outer_iter, int stride_offset) {
  for (int o_iter = 0; o_iter < outer_iter; o_iter++) {
    int index = o_iter;
    for (int i_iter = 0; i_iter < inner_iter - 1; i_iter++, index += stride_offset) {
      (array_last - index)->n = array_last - index - stride_offset;
    }
    (array_last - index)->n = array_last - o_iter - 1;
  }
  (array_last - inner_iter * outer_iter + 1)->n = array_last;
}

void Access(struct l *array, int array_size) {
  struct l *ele = array;
  for (int i = 0; i < array_size; i++) {
    __asm__ volatile("" : "+g" (*ele) : :);
    ele = ele->n;
  }
  assert(ele == array);
}
// ---------------------------------------------------------------------------------------------------
#endif  // INCLUDE_ACCESS_PATTERNS_H_
// ---------------------------------------------------------------------------------------------------
