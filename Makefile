
FLAGS=-O2

simulator: simulator.o execution.o heap.o classinfo.o tokenizer.o analyze.o
	g++ $(FLAGS) -o simulator simulator.o execution.o heap.o classinfo.o tokenizer.o analyze.o

simulator.o: simulator.cpp classinfo.h tokenizer.h
	g++ $(FLAGS)  -c simulator.cpp

analyze.o: analyze.cpp classinfo.h tokenizer.h
	g++ $(FLAGS)  -c analyze.cpp

execution.o: execution.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c execution.cpp

heap.o: heap.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c heap.cpp

classinfo.o: classinfo.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c classinfo.cpp

tokenizer.o: tokenizer.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c tokenizer.cpp

clean:
	rm -f *.o simulator
