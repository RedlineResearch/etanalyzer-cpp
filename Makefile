
FLAGS=-O2 -std=c++11 -g -Werror

simulator: simulator.o execution.o heap.o classinfo.o tokenizer.o analyze.o lastmap.o
	g++ $(FLAGS) -o simulator simulator.o execution.o heap.o classinfo.o tokenizer.o analyze.o lastmap.o

simulator.o: simulator.cpp classinfo.h tokenizer.h heap.h refstate.h lastmap.h
	g++ $(FLAGS)  -c simulator.cpp

analyze.o: analyze.cpp classinfo.h tokenizer.h
	g++ $(FLAGS)  -c analyze.cpp

execution.o: execution.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c execution.cpp

heap.o: heap.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS) -c heap.cpp

classinfo.o: classinfo.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c classinfo.cpp

tokenizer.o: tokenizer.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c tokenizer.cpp

lastmap.o: lastmap.cpp lastmap.h heap.cpp heap.h
	g++ $(FLAGS) -c lastmap.cpp

clean:
	rm -f *.o simulator
