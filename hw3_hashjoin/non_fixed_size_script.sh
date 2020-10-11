#!/usr/bin/env bash

rm ohj
make
for i in {1..2}; do ./ohj 0; done;

for i in {1..2}
do
    ./ohj 1 6
done

for i in {1..2}
do
    ./ohj 2 3 3
done

for i in {1..2}
do
    ./ohj 3 6
done

for i in {1..2}
do
    ./ohj 4 3 3
done
