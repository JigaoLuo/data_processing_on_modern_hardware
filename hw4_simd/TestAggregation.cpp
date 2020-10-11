#include "Util.hpp"
#include "aggregation.hpp"

#include <utility>
#include <random>
#include <gtest/gtest.h>
//---------------------------------------------------------------------------
struct AggregationTest: public ::testing::Test {
protected:

  void SetUp() override {
    in8 = reinterpret_cast<int8_t *>(malloc_huge(inCount * sizeof(int8_t)));
    in64 = reinterpret_cast<int64_t *>(malloc_huge(inCount * sizeof(int64_t)));

    for (int32_t i = 0; i < inCount; i++) {
      in8[i] = random() % 100;
      in64[i] = random() % 100;
    }
  }

  int8_t* in8;
  int64_t* in64;
  static constexpr int32_t inCount = 1024ull * 1024 * 128;
  static constexpr int8_t in8_serach_number = 42;
  static constexpr int64_t in64_serach_number = 42;
};
//---------------------------------------------------------------------------
TEST_F(AggregationTest, count8BrFree) {
  ASSERT_EQ(count8(in8, inCount, in8_serach_number), count8BrFree(in8, inCount, in8_serach_number));
}
//---------------------------------------------------------------------------
TEST_F(AggregationTest, count64BrFree) {
  ASSERT_EQ(count64(in64, inCount, in64_serach_number), count64BrFree(in64, inCount, in64_serach_number));
}
//---------------------------------------------------------------------------
TEST_F(AggregationTest, count8SIMD) {
  ASSERT_EQ(count8(in8, inCount, in8_serach_number), count8SIMD(in8, inCount, in8_serach_number));
}
//---------------------------------------------------------------------------
TEST_F(AggregationTest, count64SIMD) {
  ASSERT_EQ(count64(in64, inCount, in64_serach_number), count64SIMD(in64, inCount, in64_serach_number));
}
//---------------------------------------------------------------------------