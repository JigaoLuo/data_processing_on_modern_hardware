# Data Processing on Modern Hardware - Assignment 6 - Task-Level Parallelism

In Assignment 3 you have learned about different partitioning approaches for the input relations and combined them with a hash join to build a radix join, which is cache- and TLB-conscious. 

In this assignment we will further accelerate the radix join with multi-pass partitioning from Assignment 3 exploiting available parallelism on modern CPUs.

In the first part of this exercise we will focus on task-level parallelism (TLP) to take advantage of multiple cores (and, if available, multiple hardware threads) in your machine. 

In the second part of this exercise you will extend your TLP-implementation by NUMA-awareness, which we discussed in this weeks lecture. 

## Files to modify
* `ParallelRadixJoin.cpp`

## Parts
- Parallel Radix Join
- NUMA-Aware Parallel Radix Join

## Build

Build with:
```bash
$ mkdir -p build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make
```

Run with:
```bash
$ ./tester
$ ./prj
```

## Evaluation & Benchmark

[Evaluation Documentation](/Document.ipynb)