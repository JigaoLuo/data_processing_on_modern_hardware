# compiler arguments
args=-std=c++11 -pthread -W -fPIC -Wall -W -O3 -DNDEBUG -Wno-unused-parameter

# build targets starting with main
weedb: WeeDB.cpp mappedmalloc.h DBData.o BaseOperator.o OperatorsColumnar.o OperatorsVolcano.o OperatorsVector.o
	g++ ${EXP_ARGS} ${args} -o $@ WeeDB.cpp BaseOperator.o OperatorsColumnar.o OperatorsVolcano.o OperatorsVector.o DBData.o
OperatorsVector.o: BaseOperator.h Operators.h OperatorsVector.cpp primitives.h
	g++ ${args} -c -o $@ OperatorsVector.cpp

OperatorsColumnar.o: BaseOperator.h Operators.h OperatorsColumnar.cpp primitives.h
	g++ ${args} -c -o $@ OperatorsColumnar.cpp

OperatorsVolcano.o: BaseOperator.h Operators.h OperatorsVolcano.cpp
	g++ ${args} -c -o $@ OperatorsVolcano.cpp

BaseOperator.o: BaseOperator.h BaseOperator.cpp
	g++ ${args} -c -o $@ BaseOperator.cpp

DBData.o: DBData.h DBData.cpp
	g++ ${args} -c -o $@ DBData.cpp

# cleanup
clean:
	rm -f weedb *.o db.dat
