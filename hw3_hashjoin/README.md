# Data Processing on Modern Hardware - Assignment 3 - Hardware optimized hash joins

https://db.in.tum.de/teaching/ss20/dataprocessingonmodernhardware/MH_assignment_3.pdf?lang=de

The goal of this exercise is to apply some of the optimization techniques we covered in the last two weeks in the context of hash join, and analyze the cache characteristics of the original and optimized version of the code that joins two relations R and S

## Files to modify:
* OptimizedHashJoins.cpp

Hint: take a look at Relation.hpp

## Parts

- Part 1: In-memory Hash Join baseline
- Part 2: Partitioning
    - Simple naive partitioning,
    - Partitioning with “software-managed buffers” and “non-temporal writes”,
    - Multi-pass partitioning.
- Part 3: Radix Join

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
$ ./ohj
```

## Evaluation & Benchmark

[Evaluation Fixed Size Script](/fixed_size_script.sh)

[Evaluation Non-Fixed Size Script](/non_fixed_size_script.sh)

[Evaluation Documentation](/Document.ipynb)