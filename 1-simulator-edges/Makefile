
# FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0 -I./TEMP/include/igraph -L./TEMP/lib -ligraph
FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0

all: simulator-edges

simulator-edges: simulator-edges.o execution.o heap.o classinfo.o tokenizer.o  \
				summary.hpp
	g++ $(FLAGS) -o simulator-edges simulator-edges.o execution.o heap.o classinfo.o \
			tokenizer.o

simulator-edges.o: simulator-edges.cpp classinfo.h tokenizer.h heap.h refstate.h
	g++ $(FLAGS)  -c simulator-edges.cpp

execution.o: execution.cpp execution.h classinfo.h tokenizer.h
	g++ $(FLAGS) -c execution.cpp

heap.o: heap.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS) -c heap.cpp

classinfo.o: classinfo.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c classinfo.cpp

tokenizer.o: tokenizer.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c tokenizer.cpp

clean:
	rm -f *.o simulator-edges
