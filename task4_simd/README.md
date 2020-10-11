# Data Processing on Modern Hardware - Assignment 4 SIMD Vectorization

https://db.in.tum.de/teaching/ss20/dataprocessingonmodernhardware/MH_assignment_4.pdf?lang=de

## Files to modify
* `aggregation.cpp`
* `dictionaryCompression.cpp`
* `Makefile`

## Parts

- Part 1: Aggregation
- Part 2: Dictionary decompression

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
$ ./agg
$ ./dic
```

## Evaluation & Benchmark

[Evaluation Documentation](/Document.ipynb)