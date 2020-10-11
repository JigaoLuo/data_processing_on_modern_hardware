# Synchronization

https://db.in.tum.de/teaching/ss20/dataprocessingonmodernhardware/MH_assignment_5.pdf?lang=de

The goal of this exercise is to compare different synchronization apporaches introduced in this week’s lecture. 

We analyze **pessimistic and optimistic locking, exclusive and shared locks and vary the granularity from coarse- to fine-grained locking**. As workload we use a list-based set provided in the code skeleton.

## Files to modify
* `synchronization.cpp`

## Parts

1. Coarse-Grained Locking
2. Coarse-Grained Locking with read/write lock
3. Lock Coupling
4. Lock Coupling with read/write locks
5. Optimistic
6. Optimistic Lock Coupling

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
$ ./sync
```

## Evaluation & Benchmark

[Evaluation Documentation](/Document.ipynb)