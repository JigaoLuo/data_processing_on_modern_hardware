//
// Created by Alexander Beischl on 2020-05-19.
//
#include <random>
#include <cassert>
#include <unordered_set>
#include <numeric>
#include <iostream>
#include <iomanip>
#include "Relation.hpp"

using Partition_Function = void (const partition &p_in, const SplitHelper &split_helper, partition &p_out);

// TODO: We can use a better hash function: murmur hashing, fibonacci hashing
const std::hash<keyType> HASH_FUNCTION;

/// Normal hash join with assumption that relation r is smaller
/// @return number of matched tuples
uint64_t hash_join(const relation& r, const relation& s) {
  assert(r.size() <= s.size());

  // Step 1: build phase
  Timer t1 = Timer();
  std::unordered_set<keyType> hash_table;
  hash_table.reserve(r.size());
  for (const auto& tuple : r) {
    hash_table.emplace(tuple.key);
  }
  std::cout << std::fixed << "BUILD: " << std::setprecision(1) << t1.get() << std::endl;

  // Step 2: probe phase
  Timer t2 = Timer();
  uint64_t counter{0};
  for (const auto& tuple : s) {
    const auto it = hash_table.find(tuple.key);
    counter += (it != hash_table.end());  // No branching. xD
  }
  std::cout << std::fixed << "PROBE: " << std::setprecision(1) << t2.get() << std::endl;

  return counter;
}

/// Partition p_in into a new partition p_out
/// The relation in p_out must be allocated
/// @param[in] p_in input partition
/// @param[in] split_helper partition pattern
/// @param[out] p_out output partition
void partition_impl(const partition &p_in, const SplitHelper &split_helper, partition &p_out) {
  for (size_t part_id = 0; part_id < p_in.start_points.size() - 1; part_id++) {
    // Step 0 initialize histogram
    std::vector<size_t> histogram(split_helper.fanOut, 0);
    const size_t start = p_in.start_points[part_id];
    const size_t pass_of_end = p_in.start_points[part_id + 1];

    // Step 1 build histograms -> Prefix sum
    for (size_t i = start; i < pass_of_end; i++) {
      histogram[(HASH_FUNCTION(p_in.relation_[i].key) & split_helper.mask) >> split_helper.coming_bits]++;
    }
    assert(std::accumulate(histogram.begin(), histogram.end(), decltype(histogram)::value_type(0)) == pass_of_end - start);

    // Step 2 use prefix sum to partition data
    if (part_id == 0) {
      p_out.start_points.push_back(start);
    }
    for (size_t i = 1; i < split_helper.fanOut; i++) {
      assert(p_out.start_points.back() == p_out.start_points[split_helper.fanOut * part_id + i - 1]);
      p_out.start_points.push_back(p_out.start_points.back() + histogram[i - 1]);
    }
    p_out.start_points.push_back(pass_of_end);
    assert(p_out.start_points.size() == split_helper.fanOut * (part_id + 1) + 1);

    // Step 3 partition
    std::vector<size_t> pos(p_out.start_points.end() - (split_helper.fanOut + 1), p_out.start_points.end());  // local position offset book keeping
    assert(pos.size() == split_helper.fanOut + 1);
    for (size_t i = start; i < pass_of_end; i++) {
      const auto hash = (HASH_FUNCTION(p_in.relation_[i].key) & split_helper.mask) >> split_helper.coming_bits;
      p_out.relation_[pos[hash]++] = p_in.relation_[i];
    }

    // Check if all correctly copied
    for (size_t i = 1; i < split_helper.fanOut; i++) assert(pos[i - 1] == p_out.start_points[i + part_id * split_helper.fanOut]);
  }
}


/// Partition p_in into a new partition p_out with software managed buffer
/// The relation in p_out must be allocated
/// @param[in] p_in input partition
/// @param[in] split_helper partition pattern
/// @param[out] p_out output partition
void partition_software_managed_buffer_impl(const partition &p_in, const SplitHelper &split_helper, partition &p_out) {
  for (size_t part_id = 0; part_id < p_in.start_points.size() - 1; part_id++) {
    // Step 0 initialize histogram
    std::vector<size_t> histogram(split_helper.fanOut, 0);
    const size_t start = p_in.start_points[part_id];
    const size_t pass_of_end = p_in.start_points[part_id + 1];

    // Step 1 build histograms -> Prefix sum
    for (size_t i = start; i < pass_of_end; i++) {
      histogram[(HASH_FUNCTION(p_in.relation_[i].key) & split_helper.mask) >> split_helper.coming_bits]++;
    }
    assert(std::accumulate(histogram.begin(), histogram.end(), decltype(histogram)::value_type(0)) == pass_of_end - start);

    // Step 2 use prefix sum to partition data
    if (part_id == 0) {
      p_out.start_points.push_back(start);
    }
    for (size_t i = 1; i < split_helper.fanOut; i++) {
      assert(p_out.start_points.back() == p_out.start_points[split_helper.fanOut * part_id + i - 1]);
      p_out.start_points.push_back(p_out.start_points.back() + histogram[i - 1]);
    }
    p_out.start_points.push_back(pass_of_end);
    assert(p_out.start_points.size() == split_helper.fanOut * (part_id + 1) + 1);

    // Step 3 partition
    ///  Init Software Managed Buffer
    std::vector<tuple_t> buffer;
    buffer.reserve(SOFTWARE_BUFFER_SIZE * split_helper.fanOut);

    std::vector<size_t> pos(p_out.start_points.end() - (split_helper.fanOut + 1), p_out.start_points.end());  // local position offset book keeping
    assert(pos.size() == split_helper.fanOut + 1);

    /// Assume the first element is aligned
    std::vector<size_t> before_aligned_counter;
    for (size_t i = 0; i < split_helper.fanOut; i++) {
      before_aligned_counter.push_back(((32 - ((pos[i] * sizeof(tuple_t)) & 31)) / sizeof(tuple_t)) & 3);
    }
    const std::vector<size_t> before_aligned(before_aligned_counter);

    for (size_t i = start; i < pass_of_end; i++) {
      const auto hash = (HASH_FUNCTION(p_in.relation_[i].key) & split_helper.mask) >> split_helper.coming_bits;
      /// Alignment Preparation -- AVX2 32 bytes aligned
      if ((before_aligned_counter[hash] != 0) && (reinterpret_cast<uintptr_t>(&p_out.relation_[pos[hash] - SOFTWARE_BUFFER_SIZE]) & 31) != 0) {
        p_out.relation_[pos[hash]++] = p_in.relation_[i];
        before_aligned_counter[hash]--;
      } else {
        ///  Write into Software Managed Buffer
        const size_t offset_in_buffer = (pos[hash] - p_out.start_points[hash] - before_aligned[hash]) & SOFTWARE_BUFFER_MASK;
        buffer[hash * SOFTWARE_BUFFER_SIZE + offset_in_buffer] = p_in.relation_[i];

        ///  Flush Software Managed Buffer at Cache Line Boundaries == When Software Buffer Full
        if (offset_in_buffer == SOFTWARE_BUFFER_MASK) {
          assert((reinterpret_cast<uintptr_t>(&p_out.relation_[pos[hash] - SOFTWARE_BUFFER_SIZE + 1]) & 31) == 0);
          storeNontemp(reinterpret_cast<uint8_t*>(&p_out.relation_[pos[hash] - SOFTWARE_BUFFER_SIZE + 1]),
                       reinterpret_cast<uint8_t*>(&buffer[hash * SOFTWARE_BUFFER_SIZE]));
        }
        pos[hash]++;
      }
    }

    /// Flush Out Remaining Data From buffer
    for (size_t i = 0; i < split_helper.fanOut; i++) {
      const size_t num_remaining_in_buffer = (pos[i] - p_out.start_points[i] - before_aligned[i]) & SOFTWARE_BUFFER_MASK;
      for (size_t it = 0; it < num_remaining_in_buffer; it++) {
        p_out.relation_[pos[i] - num_remaining_in_buffer + it] = buffer[i * SOFTWARE_BUFFER_SIZE + it];
      }
    }

    // Check if all correctly copied
    for (size_t i = 1; i < split_helper.fanOut; i++) assert(pos[i - 1] == p_out.start_points[i + part_id * split_helper.fanOut]);
  }
}

/// Naive partition (1 pass) radix hash join with assumption that relation r is smaller
/// @return number of matched tuples
uint64_t partition_naive_radix_hash_join(const relation &r, const relation &s, uint64_t first_pass_bits, Partition_Function partition_function) {
  assert(r.size() <= s.size());

  double partition_time{0};
  Timer t1 = Timer();
  // Step 0: Init all partitions
  // The Only Pass
  SplitHelper split_helper(first_pass_bits);

  const size_t r_size = r.size();
  /// In
  partition partition_r_in(std::move(r));
  partition_r_in.start_points.push_back(0); partition_r_in.start_points.push_back(r_size);
  /// Out
  partition partition_r_out;
  partition_r_out.relation_.reserve(r_size);  // Here only reserve memory, I do not push_back to add elements.
  partition_function(partition_r_in, split_helper, partition_r_out);

  const size_t s_size = s.size();
  /// In
  partition partition_s_in(std::move(s));
  partition_s_in.start_points.push_back(0); partition_s_in.start_points.push_back(s_size);
  /// Out
  partition partition_s_out;
  partition_s_out.relation_.reserve(s_size);  // Here only reserve memory, I do not push_back to add elements.
  partition_function(partition_s_in, split_helper, partition_s_out);

  partition_time = t1.get();
  std::cout << std::fixed << "PARTITION: " << std::setprecision(1) << partition_time;

  // Step 1: Iterator all partitions
  uint64_t counter{0};
  double build_time{0};
  double probe_time{0};
  for (size_t i = 0; i < split_helper.fanOut; i++) {
    // Step 1.1: build phase
    Timer tb = Timer();
    std::unordered_set<keyType> hash_table;
    hash_table.reserve(partition_r_out.start_points[i + 1] - partition_r_out.start_points[i]);
    for (size_t r_iter = partition_r_out.start_points[i]; r_iter < partition_r_out.start_points[i + 1]; r_iter++) {
      hash_table.emplace(partition_r_out.relation_[r_iter].key);
    }
    build_time += tb.get();

    // Step 1.2: probe phase
    Timer tp = Timer();
    for (size_t s_iter = partition_s_out.start_points[i]; s_iter < partition_s_out.start_points[i + 1]; s_iter++) {
      const auto it = hash_table.find(partition_s_out.relation_[s_iter].key);
      counter += (it != hash_table.end());  // No branching. xD
    }
    probe_time += tp.get();
  }
  std::cout << std::fixed << "  BUILD: " << std::setprecision(1) << build_time;
  std::cout << std::fixed << "  PROBE: " << std::setprecision(1) << probe_time << " | " << (partition_time + build_time + probe_time) << std::endl;
  return counter;
}

/// Multi-pass partition (2 pass) hash join with assumption that relation r is smaller
/// @return number of matched tuples
uint64_t partition_multiPass_radix_hash_join(relation &r, relation &s, uint64_t first_pass_bits, uint64_t second_pass_bits, Partition_Function partition_function) {
  assert(r.size() <= s.size());

  double partition_time{0};
  Timer t1 = Timer();
  // Step 0: Init all partitions
  // First Pass
  SplitHelper first_pass_split_helper(first_pass_bits, second_pass_bits);

  const size_t r_size = r.size();
  /// In
  partition partition_r_in(std::move(r));
  partition_r_in.start_points.push_back(0); partition_r_in.start_points.push_back(r_size);
  /// Out -- First Pass
  partition partition_r_first_out;
  partition_r_first_out.relation_.reserve(r_size);  // Here only reserve memory, I do not push_back to add elements.
  partition_function(partition_r_in, first_pass_split_helper, partition_r_first_out);

  const size_t s_size = s.size();
  /// In
  partition partition_s_in(std::move(s));
  partition_s_in.start_points.push_back(0); partition_s_in.start_points.push_back(s_size);
  /// Out -- First Pass
  partition partition_s_first_out;
  partition_s_first_out.relation_.reserve(s_size);  // Here only reserve memory, I do not push_back to add elements.
  partition_function(partition_s_in, first_pass_split_helper, partition_s_first_out);

  // Second Pass
  SplitHelper second_pass_split_helper(second_pass_bits);
  /// Out -- Second Pass
  partition partition_r_second_out;
  partition_r_second_out.relation_.reserve(r_size);  // Here only reserve memory, I do not push_back to add elements.
  partition_function(partition_r_first_out, second_pass_split_helper, partition_r_second_out);

  /// Out -- Second Pass
  partition partition_s_second_out;
  partition_s_second_out.relation_.reserve(s_size);  // Here only reserve memory, I do not push_back to add elements.
  partition_function(partition_s_first_out, second_pass_split_helper, partition_s_second_out);
  partition_time = t1.get();
  std::cout << std::fixed << "PARTITION: " << std::setprecision(1) << partition_time;


  // Step 1: Iterator all partitions
  uint64_t counter{0};
  double build_time{0};
  double probe_time{0};
  for (size_t i = 0; i < first_pass_split_helper.fanOut * second_pass_split_helper.fanOut; i++) {
    // Step 1.1: build phase
    Timer tb = Timer();
    std::unordered_set<keyType> hash_table;
    hash_table.reserve(partition_r_second_out.start_points[i + 1] - partition_r_second_out.start_points[i]);
    for (size_t r_iter = partition_r_second_out.start_points[i]; r_iter < partition_r_second_out.start_points[i + 1]; r_iter++) {
      hash_table.emplace(partition_r_second_out.relation_[r_iter].key);
    }
    build_time += tb.get();

    // Step 1.2: probe phase
    Timer tp = Timer();
    for (size_t s_iter = partition_s_second_out.start_points[i]; s_iter < partition_s_second_out.start_points[i + 1]; s_iter++) {
      const auto it = hash_table.find(partition_s_second_out.relation_[s_iter].key);
      counter += (it != hash_table.end());  // No branching. xD
    }
    probe_time += tp.get();
  }

  std::cout << std::fixed << "  BUILD: " << std::setprecision(1) << build_time;
  std::cout << std::fixed << "  PROBE: " << std::setprecision(1) << probe_time << " | " << (partition_time + build_time + probe_time) << std::endl;
  return counter;
}

