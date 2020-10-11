#include "OptimizedHashJoins.hpp"

#include <time.h>
#include <iostream>
#include <iomanip>

int main(int argc, char *argv[]) {
    if (argc < 2) { std::cout << "Needs more than 2 program parameters."; return 1; }

    // Set Up
    relation r;
    relation s;
    static constexpr size_t RELATION_SIZE = 1 << 19;
//  static constexpr size_t RELATION_SIZE = 1 << 10;  // Key size: 64 KiB (row-oriented relation has larger size)
//  static constexpr size_t RELATION_SIZE = 1 << 20;  // Key size: 64 MiB (row-oriented relation has larger size)
//    static constexpr size_t RELATION_SIZE = 1 << 23;  // Key size: 512 MiB (row-oriented relation has larger size)
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

    // Hash Join
//    timespec t_start, t_end;
//    clock_gettime (CLOCK_REALTIME, &t_start);
//    clock_gettime (CLOCK_REALTIME, &t_end);
//    std::cout <<"Took " << ((t_end.tv_sec * 1000000000L + t_end.tv_nsec) - (t_start.tv_sec * 1000000000L + t_start.tv_nsec)) << " nsec.\n";
      double time{0};
      size_t num_match = 0;
      int join = strtol(argv[1], nullptr, 0);
      uint64_t first_pass_bits{0};
      if (argc == 3 && (join == 1 || join == 3)) {
        first_pass_bits = strtol(argv[2], nullptr, 0);
      }
      uint64_t second_pass_bits{0};
      if (argc == 4 && (join == 2 || join == 4)) {
        first_pass_bits = strtol(argv[2], nullptr, 0);
        second_pass_bits = strtol(argv[3], nullptr, 0);
      }
      Timer t = Timer();

      // 0: Trivial Hash Join
      // 1: Partition Navie Hash Join
      // 2: Partition MultiPass Hash Join
      // 3: Partition Navie Hash Join Software Buffer
      // 4: Partition Multi Pass Hash Join Software Buffer
      switch (join) {
      case 0: {
        num_match = hash_join(r, s);
        break;
      }
      case 1: {
        num_match = partition_naive_radix_hash_join(r, s, first_pass_bits, partition_impl);
        break;
      }
      case 2: {
        num_match = partition_multiPass_radix_hash_join(r, s, first_pass_bits, second_pass_bits, partition_impl);
        break;
      }
      case 3: {
        num_match = partition_naive_radix_hash_join(r, s, first_pass_bits, partition_software_managed_buffer_impl);
        break;
      }
      case 4: {
        num_match = partition_multiPass_radix_hash_join(r, s, first_pass_bits, second_pass_bits, partition_software_managed_buffer_impl);
        break;
      }
      }
      time = t.get();

      switch (join) {
      case 0: {
        std::cout << "TrivialHashJoin: num_match == " << num_match << " | ";
        std::cout << std::fixed << std::setprecision(1) << time << std::endl;
        break;
      }
      case 1: {
        std::cout << "PartitionNavieHashJoin: bits " << first_pass_bits <<  " | " << "num_match == " << num_match << " | ";
        std::cout << std::fixed << std::setprecision(1) << time << std::endl;
        break;
      }
      case 2: {
        std::cout << "PartitionMultiPassHashJoin: first pass bits " << first_pass_bits << ", second pass bits " << second_pass_bits << " | " << "num_match == " << num_match << " | ";
        std::cout << std::fixed << std::setprecision(1) << time << std::endl;
        break;
      }
      case 3: {
        std::cout << "PartitionNavieHashJoinSoftwareBuffer: bits " << first_pass_bits <<  " | " << "num_match == " << num_match << " | ";
        std::cout << std::fixed << std::setprecision(1) << time << std::endl;
        break;
      }
      case 4: {
        std::cout << "PartitionMultiPassHashJoinSoftwareBuffer: first pass bits " << first_pass_bits << ", second pass bits " << second_pass_bits << " | " << "num_match == " << num_match << " | ";
        std::cout << std::fixed << std::setprecision(1) << time << std::endl;
        break;
      }
      }

  // TODO: performance break-down into partition, build, probe parts
}

