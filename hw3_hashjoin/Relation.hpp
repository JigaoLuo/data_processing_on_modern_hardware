//
// Created by Alexander Beischl on 2020-05-19.
//

#ifndef HW3_RELATION_HPP
#define HW3_RELATION_HPP
//---------------------------------------------------------------------------
#include <immintrin.h>
#include <stdint.h>

#include <vector>
#include <chrono>

using keyType   = uint64_t;
using valueType = uint64_t;

static const constexpr int payload_count = 7;

/// Tuple type as naive row storage
struct alignas(64) tuple_t {
   /// Key
   keyType key;
   /// Values
   valueType value[payload_count]{0};

   tuple_t(keyType key_) : key{key_} {};

   bool operator<(const tuple_t& other) const {
     return (key < other.key);
   }

   tuple_t& operator=(const tuple_t& other) {
      key = other.key;
      return *this;
   }
};

static_assert(sizeof(tuple_t) == 64);

using relation = std::vector<tuple_t>;

/// A partition
struct partition {
   /// Relation for data strorage
   relation relation_;
   /// Partition metadata containing last element -- the size of the relation as the past-of-end iterator
   std::vector<size_t> start_points; // Vector containing the starts of the partitions

   partition() = default;
   partition(relation relation_) : relation_(relation_) {};
};
//---------------------------------------------------------------------------
/// Helper struct for partitioning the tuples
struct SplitHelper {
   /// fan out :== number of partitions
   const uint64_t fanOut;
   /// Mask for the actually partition bit pattern
   const uint64_t mask;
   /// Coming bits for multipass partition
   const uint64_t coming_bits;

   /// Constructor for single pass partition
   SplitHelper(uint64_t bits) : fanOut(1 << bits), mask{fanOut - 1}, coming_bits{0} {};

   /// Constructor for multiple passes
   /// Actually partition by mask := ((1 << bits) - 1) << coming_bits
   /// @param bits partition bit in this pass
   /// @param coming_bits all partition bits in future passes
   SplitHelper(uint64_t bits, uint64_t coming_bits) : fanOut(1 << bits), mask{(fanOut - 1) << coming_bits}, coming_bits{coming_bits} {}
};
//---------------------------------------------------------------------------
/// Helper struct for the software-managed buffers
static constexpr size_t CACHE_LINE_BYTES            = 64;
static constexpr size_t SOFTWARE_BUFFER_SIZE        = CACHE_LINE_BYTES / sizeof(tuple_t); // number of tuples per cache line TODO: is key or tuple
static constexpr size_t SOFTWARE_BUFFER_MASK        = SOFTWARE_BUFFER_SIZE - 1;

union CacheLineBuffer {
   char raw[64];
   tuple_t tuples[SOFTWARE_BUFFER_SIZE];
   uint64_t bits[8] = {0, 0, 0, 0, 0, 0, 0, 0};
};
//---------------------------------------------------------------------------
/// Helper function for the non-temporal writes
/// Non temporal write of cache line
static void __attribute__((always_inline)) inline
storeNontemp(uint8_t *dst, uint8_t *src, unsigned cnt = 1) {
   while (cnt--) {
#ifdef __AVX512F__
      assert((reinterpret_cast<uintptr_t>(dst) & 63) == 0);
      __m512i *d = (__m512i *)dst;
      __m512i *s = ((__m512i *)src);

      _mm512_stream_si512(d, *s);
#elif defined(__AVX__)
      assert((reinterpret_cast<uintptr_t>(dst) & 31) == 0);
      __m256i *d1 = (__m256i *)dst;
      __m256i *s1 = ((__m256i *)src);
      __m256i *d2 = d1 + 1;
      __m256i *s2 = (((__m256i *)src) + 1);

      _mm256_stream_si256(d1, *s1);
      _mm256_stream_si256(d2, *s2);
#else
      auto dstCL = reinterpret_cast<CacheLineBuffer *>(dst);
      *dstCL = *(CacheLineBuffer *)src;
#endif
      dst += sizeof(CacheLineBuffer);
      src += sizeof(CacheLineBuffer);
   }
}
//---------------------------------------------------------------------------
/**
  * @brief Class to time executions.
  */
class Timer {
public:
  std::chrono::time_point<std::chrono::high_resolution_clock> start;

  /**
    * @brief Take timestamp at a position in the code
    */
  Timer() {
    start = std::chrono::high_resolution_clock::now();
  }

  /**
    * @brief Get milliseconds from timestamp to now
    */
  double get() {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    return diff.count() * 1000;
  }
};
//---------------------------------------------------------------------------
#endif  // HW3_RELATION_HPP
