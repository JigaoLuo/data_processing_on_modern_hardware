# Data Processing on Modern Hardware - Assignment 2: Processing Models

https://db.in.tum.de/teaching/ss20/dataprocessingonmodernhardware/MH_assignment_2.pdf?lang=de

## Vector-at-a-time

For this assignment we have provided a sample code-base WeeDB of a minimal variant of an in-memory database system, which is suitable for an analysis of different execution models. 

The code-base contains the operators: **scan, selection and aggregation** for the processing models we covered in class: **iterator model, materialization model and vectorization model**. 

In order to execute the queries, the query plans are first specified as a hierarchy of relational operators. 

In a full-fledged RDBMS, these plans would be generated with the help of the SQL parser and an optimizer

### Files to modify:
* BaseOperator.h
* Operators.h
* OperatorsVector.cpp
* WeeDB.cpp

Hint: take a look at primitives.h

## Description of this project:

This is a program to analyze different database execution
engines. Compile by executing the command

  make

from within the sourcecode folder and use 'make clean' to 
start with a fresh build. To execute the program run

  ./weedb

or use './weedb vol' or './weedb op' to use specific execution
techniques, i.e. the former for tuple-at-a-time (Volcano) and the
latter for operator-at-a-time. The program output contains query 
timing and results.

The database is kept in the memory mapped file 'db.dat', which is
generated on the first run. This allows consecutive executions 
without data re-generation. To free space or to generate new data 
you can simply delete the file. To use the program without memory 
mapped files, you can adjust the functionality from the file
'DBData.cpp' to work on plain arrays.

For query execution, you can specify different queries as 
chain/tree of relational operators. E.g. for the query

  SELECT SUM(x) FROM rel WHERE x < 10

you can execute the query plan

  RelOperator* root = new AggregationOp ( AggregationOp::SUM,
    new SelectionOp ( SelectionOp::PredicateType::SMALLER, 10,
        new ScanOp ( rel.r, rel.len )
    )
  );


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
$ ./weedb
```

## Evaluation & Benchmark

[Evaluation Script](/script.sh)

[Evaluation Documentation](/Document.ipynb)