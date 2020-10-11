#include "ParallelRadixJoins.hpp"

#include <utility>
#include <random>
#include <unordered_set>
#include <gtest/gtest.h>
#include <vector>
//---------------------------------------------------------------------------
struct HashJoinTest: public ::testing::Test {
protected:

  void SetUp() override {
    static constexpr size_t RELATION_SIZE = 1 << 12;
    std::mt19937 gen(42);
    std::uniform_int_distribution<keyType> uni(std::numeric_limits<keyType>::min(), RELATION_SIZE << 4);

    std::unordered_set<keyType> r_set;
    while (r_set.size() < RELATION_SIZE) {
      r_set.emplace(uni(gen));
    }

    r.reserve(RELATION_SIZE);
    std::vector<keyType> r_key;
    r_key.reserve(RELATION_SIZE);

    for (const auto& it : r_set) {
      r.emplace_back(it, 0);
      r_key.emplace_back(it);
    }

    std::unordered_set<keyType> s_set;
    while (s_set.size() < RELATION_SIZE) {
      s_set.emplace(uni(gen));
    }

    s.reserve(RELATION_SIZE);
    std::vector<keyType> s_key;
    s_key.reserve(RELATION_SIZE);

    for (const auto& it : s_set) {
      s.emplace_back(it, 0);
      s_key.emplace_back(it);
    }

    std::sort(r_key.begin(), r_key.end());
    std::sort(s_key.begin(), s_key.end());

    std::vector<keyType> intersection;
    intersection.reserve(RELATION_SIZE);
    std::set_intersection(r_key.begin(), r_key.end(), s_key.begin(), s_key.end(), std::back_inserter(intersection));
    num_match = intersection.size();
  }

  relation r;
  relation s;
  size_t num_match = 0;

  std::vector<uint8_t> bits_vec = {0, 4, 8};
  std::vector<uint8_t> threads_vec = {1, 4, 8, 16, 32};
};
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, TrivialHashJoin) {
  ASSERT_EQ(num_match, hash_join(r, s));
  std::cout << "TrivialHashJoin: num_match == " << num_match << std::endl;
}
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, ParallelHashJoin) {
  for (const auto bits : bits_vec) {
    for (const auto threads : threads_vec) {
      ASSERT_EQ(num_match, parallel_radix_join(r, s, bits, threads));
      std::cout << "ParallelHashJoin: num_match == " << num_match << std::endl;
    }
  }
}
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, NumaHashJoin) {
  for (const auto bits : bits_vec) {
    for (const auto threads : threads_vec) {
      ASSERT_EQ(num_match, numa_radix_join(r, s, bits, threads));
      std::cout << "NumaHashJoin: num_match == " << num_match << std::endl;
    }
  }
}
//---------------------------------------------------------------------------