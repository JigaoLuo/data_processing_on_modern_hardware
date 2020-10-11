#include "OptimizedHashJoins.hpp"

#include <utility>
#include <random>
#include <unordered_set>
#include <gtest/gtest.h>
//---------------------------------------------------------------------------
struct HashJoinTest: public ::testing::Test {
protected:

  void SetUp() override {
    static constexpr size_t RELATION_SIZE = 1 << 20;  // Key size: 8 MiB (row-oriented relation has larger size)
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
      r.emplace_back(it);
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
      s.emplace_back(it);
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

};
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, TrivialHashJoin) {
  ASSERT_EQ(num_match, hash_join(r, s));
  std::cout << "TrivialHashJoin: num_match == " << num_match << std::endl;
}
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, PartitionNavieHashJoin) {
  // This test checks number of partition bits [1, 19].
  // This range depends on the size of dataset. If we have larger dataset, then we can raise the upper bound of the range.
  for (uint64_t bits = 1; bits < 20; bits++) {
    relation r_copy(r); relation s_copy(s);
    ASSERT_EQ(num_match, partition_naive_radix_hash_join(r_copy, s_copy, bits, partition_impl));
    std::cout << "PartitionNavieHashJoin: bits " << bits <<  " | " << "num_match == " << num_match << std::endl;
  }
}
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, PartitionMultiPassHashJoin) {
  // This test checks number of first pass partition bits [1, 9] and second pass partition bits [1, 9].
  // This range depends on the size of dataset. If we have larger dataset, then we can raise the upper bound of the range.
  for (uint64_t first_pass_bits = 1; first_pass_bits < 10; first_pass_bits++) {
    for (uint64_t second_pass_bits = 1; second_pass_bits < 10; second_pass_bits++) {
      relation r_copy(r);
      relation s_copy(s);
      ASSERT_EQ(num_match, partition_multiPass_radix_hash_join(r_copy, s_copy, first_pass_bits, second_pass_bits, partition_impl));
      std::cout << "PartitionMultiPassHashJoin: first pass bits " << first_pass_bits << ", second pass bits " << second_pass_bits << " | " << "num_match == " << num_match << std::endl;
    }
  }
}
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, PartitionNavieHashJoinSoftwareBuffer) {
  // This test checks number of partition bits [1, 19] with software buffer.
  // This range depends on the size of dataset. If we have larger dataset, then we can raise the upper bound of the range.
  for (uint64_t bits = 1; bits < 20; bits++) {
    relation r_copy(r); relation s_copy(s);
    ASSERT_EQ(num_match, partition_naive_radix_hash_join(r_copy, s_copy, bits, partition_software_managed_buffer_impl));
    std::cout << "PartitionNavieHashJoinSoftwareBuffer: bits " << bits <<  " | " << "num_match == " << num_match << std::endl;
  }
}
//---------------------------------------------------------------------------
TEST_F(HashJoinTest, PartitionMultiPassHashJoinSoftwareBuffer) {
  // This test checks number of first pass partition bits [1, 9] and second pass partition bits [1, 9] with software buffer.
  // This range depends on the size of dataset. If we have larger dataset, then we can raise the upper bound of the range.
  for (uint64_t first_pass_bits = 1; first_pass_bits < 10; first_pass_bits++) {
    for (uint64_t second_pass_bits = 1; second_pass_bits < 10; second_pass_bits++) {
      relation r_copy(r);
      relation s_copy(s);
      ASSERT_EQ(num_match, partition_multiPass_radix_hash_join(r_copy, s_copy, first_pass_bits, second_pass_bits, partition_software_managed_buffer_impl));
      std::cout << "PartitionMultiPassHashJoinSoftwareBuffer: first pass bits " << first_pass_bits << ", second pass bits " << second_pass_bits << " | " << "num_match == " << num_match << std::endl;
    }
  }
}
//---------------------------------------------------------------------------