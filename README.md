# Data Processing On Modern Hardware's Lab

## Introduction

IN310003 Selected Topics in Databases and Information Systems: Data Processing On Modern Hardware, SS 2020, TUM

Website: https://db.in.tum.de/teaching/ss20/dataprocessingonmodernhardware/

## Dependency

- [CMake](https://cmake.org/)
- [perfevent](https://github.com/viktorleis/perfevent)

## Tasks

### [Task 01: Cache Awareness](/task1_cache_awareness/)

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

### [Task 02: Processing Models](/task2_processing_models/)

Build with:
```bash
$ mkdir -p build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make
```

Run with:
```bash
$ ./weedb
```

### [Task 03: Hash Joins](/task3_hashjoin/)

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

### [Task 04: SIMD](/task4_simd/)

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

### [Task 05: Synchronization](/task5_synchronization/)

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

### [Task 06: Parallelization and NUMA-Awareness](task6_parallelization_and_numa_awareness/)

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