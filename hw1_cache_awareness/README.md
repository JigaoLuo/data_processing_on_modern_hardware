# Data Processing on Modern Hardware - Assignment 1 - Cache Level Micro-Benchmark

http://db.in.tum.de/teaching/ss20/dataprocessingonmodernhardware/MH_assignment_1.pdf?lang=en

The main goal of this assignment is to **measure the latency of each level of the memory hierarchy of your machine**. 

To achieve that, you need to write a short **micro-benchmark** that accesses the system’s memory with specific access patterns. To do that you need to create an array in main memory and initialize it appropriately.

Then the elements of the array should be accessed in one of the following patterns:
- sequential
- sequential, but in reverse order
- random (from the perspective of the hardware prefetcher)

## Hint

In the article “What Every Programmer Should Know About Memor” by Ulrich Drepper, you can find a lot of information for implementing the micro-benchmark and interpreting the results.

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
$ ./perfevent
$ ./google_bm
$ ./cyclecounter
```

## Evaluation & Benchmark

[Google Benchmark](/google_bench)

[Evaluation Documentation](/Document.ipynb)
