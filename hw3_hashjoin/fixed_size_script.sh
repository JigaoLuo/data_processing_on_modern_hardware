#!/usr/bin/env bash

rm ohj
make
for i in {1..5}; do ./ohj 0; done;

for i in {1..20}
do
    ./ohj 1 $i
    ./ohj 1 $i
    ./ohj 1 $i
done

for i in {1..10}
do
    ./ohj 2 $i $i
    ./ohj 2 $i $i
    ./ohj 2 $i $i
    ./ohj 2 $i $((i+1))
    ./ohj 2 $i $((i+1))
    ./ohj 2 $i $((i+1))
done

for i in {1..20}
do
    ./ohj 3 $i
    ./ohj 3 $i
    ./ohj 3 $i
done

for i in {1..10}
do
    ./ohj 4 $i $i
    ./ohj 4 $i $i
    ./ohj 4 $i $i
    ./ohj 4 $i $((i+1))
    ./ohj 4 $i $((i+1))
    ./ohj 4 $i $((i+1))
done
