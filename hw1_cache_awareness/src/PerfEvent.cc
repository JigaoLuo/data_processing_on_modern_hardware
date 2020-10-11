#include "PerfEvent.hpp"
#include "access_patterns.h"

int main() {
  PerfEvent e;

  static_assert(sizeof(l) == 8);

  const int stride_size = 8;
  const int working_set_size = 1 << 19;


  const int array_size = working_set_size / 8;
  const int stride_offset = stride_size / sizeof(l);


  l array[array_size];
  const int inner_iter = array_size / stride_offset;
  const int outer_iter = array_size / inner_iter;



  Init_Sequential(array, inner_iter, outer_iter, stride_offset);

  e.startCounters();


  Access(array, array_size);


  e.stopCounters();
  e.printReport(std::cout, 100); // use n as scale factor
  std::cout << std::endl;
  return 0;
}

