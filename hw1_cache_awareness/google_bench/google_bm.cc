#include "access_patterns.h"
#include "benchmark/benchmark.h"

// ---------------------------------------------------------------------------
namespace {
// ---------------------------------------------------------------------------
void BM_Random(benchmark::State &state) {
  const int working_set_size = state.range(0);
  static_assert(sizeof(l) == 8);
  const int array_size = working_set_size / 8;
  assert(array_size != 0 && (array_size & (array_size - 1)) == 0);  // check if power of 2, https://stackoverflow.com/a/600306/10971650

//  l array[array_size];
  struct l *array = (struct l *)malloc(array_size * sizeof(struct l));

  Init_Random(array, array_size);

  for (auto _ : state) {
    Access(array, array_size);
  }

  state.SetItemsProcessed(state.iterations() * array_size);
  state.SetBytesProcessed(state.iterations() * working_set_size);

  state.counters["Working Set"] = working_set_size;
}

void BM_Sequential(benchmark::State &state) {
  const int working_set_size = state.range(0);
  static_assert(sizeof(l) == 8);
  const int array_size = working_set_size / 8;
  const int stride_size = state.range(1);
  const int stride_offset = stride_size / sizeof(l);
  assert(array_size > stride_offset);
  assert(array_size != 0 && (array_size & (array_size - 1)) == 0);  // check if power of 2, https://stackoverflow.com/a/600306/10971650
  assert(stride_offset != 0 && (stride_offset & (stride_offset - 1)) == 0);  // check if power of 2, https://stackoverflow.com/a/600306/10971650

//  l array[array_size];
  struct l *array = (struct l *)malloc(array_size * sizeof(struct l));

  const int inner_iter = array_size / stride_offset;
  const int outer_iter = array_size / inner_iter;

  Init_Sequential(array, inner_iter, outer_iter, stride_offset);

  for (auto _ : state) {
    Access(array, array_size);
  }

  state.SetItemsProcessed(state.iterations() * array_size);
  state.SetBytesProcessed(state.iterations() * working_set_size);

  state.counters["Working Set"] = working_set_size;
  state.counters["Stride"] = stride_size;
}

void BM_Rev_Sequential(benchmark::State &state) {
  const int working_set_size = state.range(0);
  static_assert(sizeof(l) == 8);
  const int array_size = working_set_size / 8;
  const int stride_size = state.range(1);
  const int stride_offset = stride_size / sizeof(l);
  assert(array_size > stride_offset);
  assert(array_size != 0 && (array_size & (array_size - 1)) == 0);  // check if power of 2, https://stackoverflow.com/a/600306/10971650
  assert(stride_offset != 0 && (stride_offset & (stride_offset - 1)) == 0);  // check if power of 2, https://stackoverflow.com/a/600306/10971650

//  l array[array_size];
  struct l *array = (struct l *)malloc(array_size * sizeof(struct l));

  const int inner_iter = array_size / stride_offset;
  const int outer_iter = array_size / inner_iter;

  Init_Rev_Sequential(array + array_size - 1, inner_iter, outer_iter, stride_offset);

  for (auto _ : state) {
    Access(array + array_size - 1, array_size);
  }

  state.SetItemsProcessed(state.iterations() * array_size);
  state.SetBytesProcessed(state.iterations() * working_set_size);

  state.counters["Working Set"] = working_set_size;
  state.counters["Stride"] = stride_size;
}
// ---------------------------------------------------------------------------
}  // namespace
// ---------------------------------------------------------------------------
BENCHMARK(BM_Random)-> Range(1 << 10, 1 << 21);
BENCHMARK(BM_Sequential)-> Ranges({{1 << 14, 1 << 21}, {8, 1 << 12}});
BENCHMARK(BM_Rev_Sequential)-> Ranges({{1 << 14, 1 << 21}, {8, 1 << 12}});
// ---------------------------------------------------------------------------
int main(int argc, char **argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
}
// ---------------------------------------------------------------------------
