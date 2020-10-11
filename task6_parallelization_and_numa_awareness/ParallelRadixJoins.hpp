//
// Created by Alexander Beischl on 2020-05-19.
//
#include "Relation.hpp"
#include "ThreadPool.hpp"
#include "Barrier.hpp"

#include <numa.h>
#include <numaif.h>

#include <cassert>
#include <unordered_set>
#include <vector>
#include <thread>
#include <atomic>

enum class Partitioning { naive, softwareManaged, multiPass };

// Assume relation r is smaller
uint64_t hash_join(const relation &r, const relation &s) {
  uint64_t matches = 0;
  std::unordered_set<keyType> hashTable;
  hashTable.reserve(r.size());

  // Build the hash table by inserting all tuples from r
  for (auto &t : r) {
    hashTable.insert(t.key);
  }
  // Probe the hash table for each element of s
  for (auto &t : s) {
    matches += hashTable.count(t.key);
  }
  return matches;
}

// Helper function, needed to reuse it for multi-pass partitioning
// Shift can be used for the multi-pass partitioning
partition partition_naive(const relation &r, size_t start, size_t end, uint8_t bits, uint8_t shift = 0) {
  SplitHelper split(bits); // Define the partitioning

  // Step 1: Build histograms to derive the prefix sums
  std::vector<uint64_t> histogram(split.fanOut, 0);

  for (size_t i = start; i < end; i++) {
    // If needed: shift the key. Apply the split mask to determine the partition
    const size_t bucket = (r[i].key >> shift) & split.mask;
    histogram[bucket]++;
  }

  // Step 2: Use prefix sum to partition data
  uint64_t prefSum = 0;
  std::vector<uint64_t> startPos(split.fanOut, 0); // Store partition starts

  for (size_t i = 0; i < split.fanOut; i++) {
    startPos[i] = prefSum;   // Store the offset of this buckets array
    prefSum += histogram[i]; // Update the offset of the next bucket
  }

  // Step 3 partition the data
  relation parts(prefSum); // Relation to store the partitioned data
  std::vector<uint64_t> offset(split.fanOut, 0); // Vector of partition offsets

  for (size_t i = start; i < end; i++) {
    auto bucket = (r[i].key >> shift) & split.mask; // Determine partition
    auto pos = startPos[bucket] + offset[bucket];   // Determine position
    parts[pos] = r[i]; // Write the tuple to the position
    offset[bucket]++;  // Update the offset of this partition
  }

  return {parts, startPos}; // Return the partitioned relation + start positions
}

partition partition_naive(const relation &r, uint8_t bits, uint8_t shift = 0) {
  return partition_naive(r, 0, r.size(), bits, shift);
}

std::pair<partition, std::vector<uint64_t>> partition_softwareManaged(const relation &r, uint8_t bits) {
  SplitHelper split(bits); // Define the partitioning

  // Step 1: Build histograms to derive the prefix sums
  std::vector<uint64_t> histogram(split.fanOut, 0);

  for (auto &t : r) {
    // Apply the split mask to determine the partition
    const uint64_t bucket = t.key & split.mask;
    histogram[bucket]++;
  }

  // Step 2: Use prefix sum to partition data
  uint64_t prefSum = 0;
  std::vector<uint64_t> startPos(split.fanOut, 0); // Store partition starts

  for (uint64_t i = 0; i < split.fanOut; i++) {
    startPos[i] = prefSum;
    auto tmp = histogram[i]; // Store the offset of this buckets array

    // Align the offsets for non-temporal writes
    if (tmp % tuplesPerCL != 0)
      tmp += tuplesPerCL - (tmp % tuplesPerCL);

    prefSum += tmp; // Update the offset of the next bucket
  }
  /// From this point on the data is not dense anymore

  // Step 3 partition
  // Initialize a software managed buffer per partition + its current offset
  std::vector<SoftwareManagedBuffer> buffers(split.fanOut);
  std::vector<uint8_t> bufferOffset(split.fanOut, 0);

  relation parts(prefSum); // Relation to store the partitioned data
  std::vector<uint64_t> offset(split.fanOut, 0); // Vector of partition offsets

  for (auto &t : r) {
    auto bucket = t.key & split.mask; // Determine the bucket
    auto &buff = buffers[bucket];     // Get the software-managed buffer
    auto &pos = bufferOffset[bucket]; // Determine the position in the buffer
    buff.tuples[pos] = t;             // Write the tuple into the buffer
    pos++;

    // Buffer full?
    if (pos == tuplesPerCL) {
      auto writePos = &parts[startPos[bucket] +
                             offset[bucket]]; // Determine pos to write to
      auto writePtr = reinterpret_cast<uint8_t *>(writePos);
      storeNontemp(writePtr, &buff); // Non-temporal write to the position
      offset[bucket] += tuplesPerCL; // Update write head
      pos = 0;                       // Update software-managed buffer offset
    }
  }

  // Handle not empty buffers
  for (uint64_t i = 0; i < split.fanOut; i++) {
    auto &buff = buffers[i];
    auto &pos = bufferOffset[i];

    for (size_t j = 0; j < pos; j++) {
      auto writePos = startPos[i] + offset[i];
      parts[writePos] = buff.tuples[j];
      offset[i]++;
    }
  }

  partition result = {parts, startPos};
  return std::make_pair(result, offset);
}

partition partition_multiPass(const relation &r, uint8_t bits1, uint8_t bits2) {

  // Partition 1. stage
  partition p1 = partition_naive(r, bits1);

  // Partition 2. stage
  relation result;                // Store the tuples of the result
  std::vector<uint64_t> startPos; // Start positions of all sub-partitions
  uint64_t offset = 0;

  for (uint64_t i = 0; i < p1.start.size(); i++) {
    auto start = p1.start[i];
    auto end = (i == p1.start.size() - 1) ? p1.r.size() : p1.start[i + 1];

    auto p2 = partition_naive(p1.r, start, end, bits2, bits1); // Partition

    for (auto e : p2.r)
      result.push_back(e); // Insert partition into result

    for (auto e : p2.start)
      startPos.push_back(offset + e); // Adapt startPos from local to global

    offset += (end - start); // Update offset by the partition's #elements
  }

  return {result, startPos};
}

uint64_t radix_join(const relation &r, const relation &s, Partitioning type, uint8_t bits) {

  uint64_t matches = 0;

  // Step 1: partitioning phase
  partition pr{};
  partition ps{};
  std::vector<uint64_t> offR{};
  std::vector<uint64_t> offS{};

  switch (type) {
    case Partitioning::naive:
      pr = partition_naive(r, bits);
      ps = partition_naive(s, bits);
      break;
    case Partitioning::multiPass:
      pr = partition_multiPass(r, bits, bits);
      ps = partition_multiPass(s, bits, bits);
      break;
    case Partitioning::softwareManaged: {
      auto tmpR = partition_softwareManaged(r, bits);
      auto tmpS = partition_softwareManaged(s, bits);
      pr = tmpR.first;
      offR = tmpR.second;
      ps = tmpS.first;
      offS = tmpS.second;
    } break;
  }

  // Step 2: partition-wise build & probe phase
  for (size_t i = 0; i < pr.start.size(); i++) {
    // Determine range of partitions
    auto start_r = pr.start[i];                                          // For r
    auto end_r = (i == pr.start.size() - 1) ? pr.r.size() : pr.start[i + 1]; // For r
    auto start_s = ps.start[i];                                          // For S
    auto end_s = (i == ps.start.size() - 1) ? ps.r.size() : ps.start[i + 1]; // For S

    if (type == Partitioning::softwareManaged) { // Handle padding
      end_r = std::min(end_r, static_cast<uint64_t>(start_r + offR[i]));
      end_s = std::min(end_s, static_cast<uint64_t>(start_s + offS[i]));
    }

    std::unordered_set<keyType> hashTable;
    hashTable.reserve(end_r - start_r);
    // Step 1: build phase
    for (auto j = start_r; j != end_r; j++) {
      hashTable.insert(pr.r[j].key);
    }

    // Step 2: probe phase
    for (auto j = start_s; j != end_s; j++) {
      matches += hashTable.count(ps.r[j].key);
    }
  }

  return matches;
}

partition parallel_partition(const relation &r, uint8_t bits1, uint8_t num_threads) {
  /// Partition 1. stage
  uint8_t shift = 0;
  SplitHelper split(bits1); // Define the partitioning

  relation parts(r.size()); // Relation to store the partitioned data
  std::vector<uint64_t> offset_stage1(split.fanOut, 0); // Vector of partition offsets

  // IMPORTANT: it is just my assumption, that each task has same number of entries
  //            the number of thread has to be the power of 2
  //            the relation size has to be the power of 2
  //            I just make this assumption to avoid the last task having differently sized chunk
  assert(r.size() % num_threads == 0);

  std::mutex global_histogram_lock;
  std::vector<uint64_t> global_histogram(split.fanOut, 0);
  std::vector<std::vector<uint64_t>> local_histograms(num_threads, std::vector<uint64_t>(split.fanOut, 0));

  {
    Barrier barrier(num_threads);
    ThreadPool<void(void)> threadpool_stage1(num_threads);

    for (size_t task_id = 0; task_id < num_threads; task_id++) {
      std::function<void(void)> f = [&local_histograms, task_id, &r, num_threads, shift, &split, &global_histogram, &global_histogram_lock, &barrier, &parts]() {
        // Step 1: Build histograms to derive the prefix sums
        std::vector<uint64_t> &local_histogram = local_histograms[task_id];
        const size_t local_start_offset = task_id * (r.size() / num_threads);
        for (size_t i = 0; i < r.size() / num_threads; i++) {
          // If needed: shift the key. Apply the split mask to determine the partition
          const size_t bucket = (r[local_start_offset + i].key >> shift) & split.mask;
          local_histogram[bucket]++;
        }

        /// Update to the global histogram
        for (size_t i = 0; i < split.fanOut; i++) {
          std::scoped_lock<std::mutex> sc{global_histogram_lock};
          global_histogram[i] += local_histogram[i];
        }

        /// Barrier
        barrier.Wait();

        // Step 2: Use prefix sum to partition data
        std::vector<uint64_t> local_startPos(split.fanOut, 0); // Store partition starts
        for (size_t i = 0; i < split.fanOut; i++) {
          uint64_t prefSum = 0;
          // Less than i
          for (size_t prev_histogram = 0; prev_histogram < i; prev_histogram++) {
            prefSum += global_histogram[prev_histogram];
          }
          // Same i, but tasks chronologically before this task
          for (size_t prev_task = 0; prev_task < task_id; prev_task++) {
            prefSum += local_histograms[prev_task][i];
          }
          local_startPos[i] = prefSum;   // Store the offset of this buckets array
        }

        /// NO Barrier   :D
        // Step 3 partition the data
        std::vector<uint64_t> local_offset(split.fanOut, 0); // Vector of partition offsets
        for (size_t i = 0; i < r.size() / num_threads; i++) {
          const size_t bucket = (r[local_start_offset + i].key >> shift) & split.mask; // Determine partition
          const size_t pos = local_startPos[bucket] + local_offset[bucket];   // Determine position
          parts[pos] = r[local_start_offset + i]; // Write the tuple to the position
          local_offset[bucket]++;  // Update the offset of this partition
        }
      };

      threadpool_stage1.addTask(std::move(f));
    }
  }


  /// To get global_startPos is just little work and non-relevant with the threading above, so I decide just to scalar code it.
  size_t tmp = 0;
  std::vector<uint64_t> global_startPos(split.fanOut, 0); // Store partition starts
  for (size_t i = 0; i < split.fanOut; i++) {
    global_startPos[i] = tmp;   // Store the offset of this buckets array
    tmp += global_histogram[i]; // Update the offset of the next bucket
  }

  /// I decide to only have SINGLE pass partition. Reason see in my document.
  return {parts, global_startPos};

//  partition p1 = {parts, global_startPos};

//  /// Partition 2. stage
//  std::vector<partition> partitions_stage2;
//  partitions_stage2.resize(p1.start.size());
//  {
//    ThreadPool<void(void)> threadpool_stage2(num_threads);
//    for (uint64_t i = 0; i < p1.start.size(); i++) {
//      auto start = p1.start[i];
//      auto end = (i == p1.start.size() - 1) ? p1.r.size() : p1.start[i + 1];
//      std::function<void(void)> f = [&partitions_stage2, &p1, start, end, bits2, bits1, i] {
//        partitions_stage2[i] = partition_naive(p1.r, start, end, bits2, bits1); // Partition
//      };
//      threadpool_stage2.addTask(std::move(f));
//    }
//  }

//  relation result;                // Store the tuples of the result
//  std::vector<uint64_t> startPos; // Start positions of all sub-partitions
//  uint64_t offset = 0;
//  for (uint64_t i = 0; i < p1.start.size(); i++) {
//    auto start = p1.start[i];
//    auto end = (i == p1.start.size() - 1) ? p1.r.size() : p1.start[i + 1];
//    const auto& p2 = partitions_stage2[i];
//    for (auto e : p2.r) {
//      result.push_back(e); // Insert partition into result
//    }
//    for (auto e : p2.start) {
//      startPos.push_back(offset + e); // Adapt startPos from local to global
//    }
//    offset += (end - start); // Update offset by the partition's #elements
//  }
//  return {result, startPos};
}

// Feel free to adapt the function signature if needed
uint64_t parallel_radix_join(const relation &r, const relation &s, uint8_t bits, uint8_t num_threads) {
  /// Implement the parallel radix join with multi-pass partitioning
  ///  using task-level parallelism (TLP) [Part 1]
  ///  Use the provided serial implementation of the multi-pass partitioning and adapt it.

  /// Step 1: partitioning phase with multi-pass partitioning
  partition pr{};
  partition ps{};
  pr = parallel_partition(r, bits, num_threads);
  ps = parallel_partition(s, bits, num_threads);

  /// Step 2: partition-wise build & probe phase
  std::atomic<uint64_t> matches = 0;
  {
    ThreadPool<void(void)> threadpool_step2(num_threads);
    assert(static_cast<size_t>(1 << bits) == pr.start.size());
    for (size_t i = 0; i < static_cast<size_t>(1 << bits) /* fan out */; i++) {
      std::function<void(void)> f = [i, &pr, &ps, &matches]() {
        // Determine range of partitions
        auto start_r = pr.start[i];                                          // For r
        auto end_r = (i == pr.start.size() - 1) ? pr.r.size() : pr.start[i + 1]; // For r
        auto start_s = ps.start[i];                                          // For S
        auto end_s = (i == ps.start.size() - 1) ? ps.r.size() : ps.start[i + 1]; // For S

        std::unordered_set<keyType> hashTable;
        hashTable.reserve(end_r - start_r);
        // Step 1: build phase
        for (auto j = start_r; j != end_r; j++) {
          hashTable.insert(pr.r[j].key);
        }

        // Step 2: probe phase
        uint64_t local_mathces = 0;
        for (auto j = start_s; j != end_s; j++) {
          local_mathces += hashTable.count(ps.r[j].key);
        }
        matches += local_mathces;
      };
      threadpool_step2.addTask(std::move(f));
    }
  }
  return matches;
}

//// TODO: return multiple partition
partition* numa_partition(const relation &r, uint8_t bits1, uint8_t num_threads) {
  /// Partition 1. stage
  uint8_t shift = 0;
  SplitHelper split(bits1); // Define the partitioning

  // https://stackoverflow.com/questions/49105427/c-numa-optimization
  /// I delete you later, after the join is done.     :D
  partition* parts_ptrs = new partition[num_threads];

  std::vector<uint64_t> offset_stage1(split.fanOut, 0); // Vector of partition offsets

  // IMPORTANT: it is just my assumption, that each task has same number of entries
  //            the number of thread has to be the power of 2
  //            the relation size has to be the power of 2
  //            I just make this assumption to avoid the last task having differently sized chunk
  assert(r.size() % num_threads == 0);

  {
    ThreadPool<void(void)> threadpool_stage1(num_threads);

    for (size_t task_id = 0; task_id < num_threads; task_id++) {
      std::function<void(void)> f = [task_id, &r, num_threads, shift, &split, &parts_ptrs]() {
        /// Step 0: configure on a NUMA node
        /// runs the current task and its children on a specific node.
        /// E5-2660 v2 hash 2 Sockets (2 NUMA node)
        /// Thread [0, 19] on Node 0 | Thread [20, 39] on Node 1 | Thread [40, 59] on Node 0 | Thread [60, 59] on Node 1 | ......
        // TODO(jigao): I am not sure. Hope this works!
        const int node_id = (num_threads / 20) & 1;
        if (numa_run_on_node(node_id) != 0) {
          printf("could not assign current thread to node %d!\n", 1);
        }
        numa_set_preferred(node_id);


        // Step 1: Build histograms to derive the prefix sums
        std::vector<uint64_t> local_histogram(split.fanOut, 0);
        const size_t local_start_offset = task_id * (r.size() / num_threads);
        for (size_t i = 0; i < r.size() / num_threads; i++) {
          // If needed: shift the key. Apply the split mask to determine the partition
          const size_t bucket = (r[local_start_offset + i].key >> shift) & split.mask;
          local_histogram[bucket]++;
        }

        /// Step 2: no communication with global and other threads.
        // Step 2: Use prefix sum to partition data
        std::vector<uint64_t> local_startPos(split.fanOut, 0); // Store partition starts
        uint64_t prefSum = 0;
        for (size_t i = 0; i < split.fanOut; i++) {
          local_startPos[i] = prefSum;   // Store the offset of this buckets array
          prefSum += local_histogram[i]; // Update the offset of the next bucket
        }

        // Step 3 partition the data
        relation local_parts(prefSum); // Relation to store the partitioned data
        std::vector<uint64_t> local_offset(split.fanOut, 0); // Vector of partition offsets
        for (size_t i = 0; i < r.size() / num_threads; i++) {
          const size_t bucket = (r[local_start_offset + i].key >> shift) & split.mask; // Determine partition
          const size_t pos = local_startPos[bucket] + local_offset[bucket];   // Determine position
          local_parts[pos] = r[local_start_offset + i]; // Write the tuple to the position
          local_offset[bucket]++;  // Update the offset of this partition
        }

        /// Step4: this workload done.
        /// initialization with respect to first touch policy
        parts_ptrs[task_id] = partition{local_parts, local_startPos};
      };

      threadpool_stage1.addTask(std::move(f));
    }
  }
  return parts_ptrs;
}

uint64_t numa_radix_join(relation &r, relation &s, uint8_t bits, uint8_t num_threads) {
  /// Implement a NUMA-aware version of your parallel radix join [Part 2]

  if(numa_available() < 0){
    printf("System does not support NUMA API!\n");
  }

  /// runs the current task and its children on a specific node.
  if (numa_run_on_node(0) != 0) {
    printf("could not assign current thread to node %d!\n", 1);
  }
  numa_set_preferred(0);

  // equivalent as following
//  nodemask_t mask;
//  nodemask_zero(&mask);
//  numa_bitmask_setbit(&mask, );
//  numa_bind(&mask);

  /// Step 1: partitioning phase with multi-pass partitioning
  partition* pr = numa_partition(r, bits, num_threads);
  partition* ps = numa_partition(s, bits, num_threads);

  /// Step 2: partition-wise build & probe phase
  std::atomic<uint64_t> matches = 0;
  {
    ThreadPool<void(void)> threadpool_step2(num_threads);
    assert(static_cast<size_t>(1 << bits) == pr[0].start.size());
    for (size_t i = 0; i < static_cast<size_t>(1 << bits); i++) {

      std::function<void(void)> f = [num_threads, i, &pr, &ps, &matches]() {
        /// Step 0: configure on a NUMA node
        /// runs the current task and its children on a specific node.
        /// E5-2660 v2 hash 2 Sockets (2 NUMA node)
        /// Thread [0, 19] on Node 0 | Thread [20, 39] on Node 1 | Thread [40, 59] on Node 0 | Thread [60, 59] on Node 1 | ......
        // TODO(jigao): I am not sure. Hope this works!
        const int node_id = (num_threads / 20) & 1;
        if (numa_run_on_node(node_id) != 0) {
          printf("could not assign current thread to node %d!\n", 1);
        }
        numa_set_preferred(node_id);

        std::unordered_set<keyType> hashTable;
        size_t reserve_size = 0;
        for (size_t part_id = 0; part_id < num_threads; part_id++) {
          auto start_r = pr[part_id].start[i];                                          // For r
          auto end_r = (i == pr[part_id].start.size() - 1) ? pr[part_id].r.size() : pr[part_id].start[i + 1]; // For r
          reserve_size += (end_r - start_r);
        }
        hashTable.reserve(reserve_size);

        for (size_t part_id = 0; part_id < num_threads; part_id++) {
            // Determine range of partitions
            auto start_r = pr[part_id].start[i];                                          // For r
            auto end_r = (i == pr[part_id].start.size() - 1) ? pr[part_id].r.size() : pr[part_id].start[i + 1]; // For r

            // Step 1: build phase
            /// Take i (aka the partition bit pattern) from all local partition from all threads
            for (auto j = start_r; j != end_r; j++) {
              hashTable.insert(pr[part_id].r[j].key);
            }
        }

        for (size_t part_id = 0; part_id < num_threads; part_id++) {
          // Determine range of partitions
          auto start_s = ps[part_id].start[i];                                          // For S
          auto end_s = (i == ps[part_id].start.size() - 1) ? ps[part_id].r.size() : ps[part_id].start[i + 1]; // For S

          // Step 2: probe phase
          uint64_t local_matches = 0;
          /// Take i (aka the partition bit pattern) from all local partition from all threads
          for (auto j = start_s; j != end_s; j++) {
            local_matches += hashTable.count(ps[part_id].r[j].key);
          }
          matches += local_matches;
        }
      };
      threadpool_step2.addTask(std::move(f));
    }
  }
  delete[](pr);
  delete[](ps);
  return matches;
}