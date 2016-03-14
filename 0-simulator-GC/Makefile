
# FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0 -I./TEMP/include/igraph -L./TEMP/lib -ligraph
FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0

all: simulator-GC

simulator-GC: simulator-GC.o execution.o heap.o classinfo.o tokenizer.o analyze.o \
				summary.hpp memorymgr.o
	g++ $(FLAGS) -o simulator-GC simulator-GC.o execution.o heap.o classinfo.o \
			tokenizer.o analyze.o memorymgr.o

simulator-GC.o: simulator-GC.cpp classinfo.h tokenizer.h heap.h refstate.h
	g++ $(FLAGS)  -c simulator-GC.cpp

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

memorymgr.o: memorymgr.cpp heap.h
	g++ $(FLAGS) -c memorymgr.cpp

clean:
	rm -f *.o simulator-GC
