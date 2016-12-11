
# FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0 -I./TEMP/include/igraph -L./TEMP/lib -ligraph
GIT=git
FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0 -I./local-include -static -L./local-lib -lstxxl_debug
.PHONY: clean gitversion

all: gitversion simulator simulator-type1 simulator-find-main

gitversion:
	$(GIT) rev-parse HEAD | awk ' BEGIN {print "#include \"version.hpp\""} {print "const char *build_git_sha = \"" $$0"\";"} END {}' > version.cpp
	date | awk 'BEGIN {} {print "const char *build_git_time = \""$$0"\";"} END {} ' >> version.cpp

simulator: simulator.o execution.o heap.o classinfo.o tokenizer.o analyze.o version.o \
			summary.hpp
	g++ $(FLAGS) -o simulator simulator.o execution.o heap.o classinfo.o tokenizer.o analyze.o version.o

simulator-type1: simulator-type1.o execution.o heap.o classinfo.o tokenizer.o analyze.o version.o \
			summary.hpp
	g++ $(FLAGS) -o simulator-type1 simulator-type1.o execution.o heap.o classinfo.o tokenizer.o analyze.o version.o

simulator-find-main: simulator-find-main.o execution.o heap.o classinfo.o tokenizer.o analyze.o version.o \
			summary.hpp
	g++ $(FLAGS) -o simulator-find-main simulator-find-main.o execution.o heap.o classinfo.o tokenizer.o analyze.o version.o

simulator.o: simulator.cpp classinfo.h tokenizer.h heap.h refstate.h
	g++ $(FLAGS)  -c simulator.cpp

simulator-find-main.o: simulator-find-main.cpp classinfo.h tokenizer.h heap.h refstate.h
	g++ $(FLAGS)  -c simulator-find-main.cpp

simulator-type1.o: simulator.cpp classinfo.h tokenizer.h heap.h refstate.h
	g++ $(FLAGS) -D ENABLE_TYPE1 -c simulator.cpp -o simulator-type1.o

analyze.o: analyze.cpp classinfo.h tokenizer.h execution.h
	g++ $(FLAGS)  -c analyze.cpp

execution.o: execution.cpp execution.h classinfo.h tokenizer.h
	g++ $(FLAGS) -c execution.cpp

heap.o: heap.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS) -c heap.cpp

classinfo.o: classinfo.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c classinfo.cpp

tokenizer.o: tokenizer.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c tokenizer.cpp

# lastmap.o: lastmap.cpp lastmap.h heap.cpp heap.h \
	g++ $(FLAGS) -c lastmap.cpp

version.o: version.cpp version.hpp
	g++ $(FLAGS) -c version.cpp

clean:
	rm -f *.o simulator
