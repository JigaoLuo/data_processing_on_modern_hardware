# ToDo adapt the compiler flags for your experiments and machine

agg: aggregation.cpp
	    g++ -march=skylake-avx512 -O3 -std=c++17 -g aggregation.cpp Util.cpp -o agg

dictComp: dictionaryCompression.cpp
	    g++ -march=skylake-avx512 -O3 -std=c++17 -g dictionaryCompression.cpp Util.cpp -o dictComp