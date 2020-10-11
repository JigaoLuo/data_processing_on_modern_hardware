#include "Util.hpp"
#include "dictionaryCompression.hpp"

#include <utility>
#include <random>
#include <gtest/gtest.h>
//---------------------------------------------------------------------------
struct DictionaryCompressionTest: public ::testing::Test {
protected:

  void SetUp() override {
    // dictionary compression
    in8 = reinterpret_cast<uint8_t *>(malloc_huge(inCount * sizeof(uint8_t)));
    out32_L = reinterpret_cast<int32_t *>(malloc_huge(inCount * 2 * sizeof(int32_t)));
    out32_R = reinterpret_cast<int32_t *>(malloc_huge(inCount * 2 * sizeof(int32_t)));

    for (int32_t i = 0; i < inCount; i++)
      in8[i] = random();
  }

  uint8_t* in8;
  static constexpr int32_t inCountBits = 23;
  static constexpr int32_t inCount = 1ull << inCountBits;
  int32_t* out32_L;
  int32_t* out32_R;
};
//---------------------------------------------------------------------------
TEST_F(DictionaryCompressionTest, dictDecompress_BYTE) {
  dictDecompress(in8, inCount, out32_L);
  dictDecompress_BYTE(in8, inCount, out32_R);
  for (int i = 0; i < inCount * 2; ++i) {
    ASSERT_EQ(out32_L[i], out32_R[i]) << "out32_L and out32_R differ at index " << i;
  }
}
//---------------------------------------------------------------------------
TEST_F(DictionaryCompressionTest, dictDecompress8) {
  dictDecompress(in8, inCount, out32_L);
  dictDecompress_BYTE(in8, inCount, out32_R);
  for (int i = 0; i < inCount * 2; ++i) {
    ASSERT_EQ(out32_L[i], out32_R[i]) << "out32_L and out32_R differ at index " << i;
  }
}
//---------------------------------------------------------------------------
TEST_F(DictionaryCompressionTest, count8SIMD) {
  dictDecompress(in8, inCount, out32_L);
  dictDecompressGather(in8, inCount, out32_R);
  for (int i = 0; i < inCount * 2; ++i) {
    EXPECT_EQ(out32_L[i], out32_R[i]) << "out32_L and out32_R differ at index " << i;
  }
}
//---------------------------------------------------------------------------
TEST_F(DictionaryCompressionTest, dictDecompressPermute) {
  dictDecompress(in8, inCount, out32_L);
  dictDecompressPermute(in8, inCount, out32_R);
  for (int i = 0; i < inCount * 2; ++i) {
    EXPECT_EQ(out32_L[i], out32_R[i]) << "out32_L and out32_R differ at index " << i;
  }
}
//---------------------------------------------------------------------------