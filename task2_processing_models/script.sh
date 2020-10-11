#!/usr/bin/env bash

make
echo "Query 0"
./weedb vol 0
./weedb op 0
./weedb vec 0

echo "Query 1"
./weedb vol 1
./weedb op 1
./weedb vec 1

echo "Query 2"
./weedb vol 2
./weedb op 2
./weedb vec 2

echo "Query 3"
./weedb vol 3
./weedb op 3
./weedb vec 3