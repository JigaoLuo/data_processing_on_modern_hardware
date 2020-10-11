#include "Util.hpp"

#include <sys/mman.h>
#include <cstdio>

// $ man mmap
// > [...] If addr is NULL, then the kernel chooses the (page-aligned) address
//       at which to create the mapping; this is the most portable method of
//       creating a new mapping. [...]
//   "page-aligned" implies 64-Bytes-aligned
void *malloc_huge(size_t size) {
  void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(p, size, MADV_HUGEPAGE);
  memset(p, 0, size);
  return p;
}

void _mm512i_print_epi64 (__m512i v) {
  auto p = reinterpret_cast<int64_t*>(&v);
  printf ( "(%ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld)\n",
           p[0],
           p[1],
           p[2],
           p[3],
           p[4],
           p[5],
           p[6],
           p[7]
  );
}

void _mm512i_print_epi32 (__m512i v) {
  auto p = reinterpret_cast<int32_t*>(&v);
  printf ( "(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)\n",
           p[0],
           p[1],
           p[2],
           p[3],
           p[4],
           p[5],
           p[6],
           p[7],
           p[8],
           p[9],
           p[10],
           p[11],
           p[12],
           p[13],
           p[14],
           p[15]
  );
}