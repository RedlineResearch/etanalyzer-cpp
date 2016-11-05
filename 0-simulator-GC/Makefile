
# FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0 -I./TEMP/include/igraph -L./TEMP/lib -ligraph
GIT=git
FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0
.PHONY: clean gitversion

all: gitversion simulator-GC simulator-livesize

gitversion:
	$(GIT) rev-parse HEAD | awk ' BEGIN {print "#include \"version.hpp\""} {print "const char *build_git_sha = \"" $$0"\";"} END {}' > version.cpp
	date | awk 'BEGIN {} {print "const char *build_git_time = \""$$0"\";"} END {} ' >> version.cpp

simulator-GC: simulator-GC.o execution.o heap.o classinfo.o tokenizer.o version.o\
				summary.hpp memorymgr.o
	g++ $(FLAGS) -o simulator-GC simulator-GC.o execution.o heap.o classinfo.o version.o \
			tokenizer.o  memorymgr.o

simulator-livesize: simulator-livesize.o execution.o heap.o classinfo.o tokenizer.o version.o \
				memorymgr.o
	g++ $(FLAGS) -o simulator-livesize simulator-livesize.o execution.o heap.o classinfo.o version.o \
			tokenizer.o memorymgr.o

simulator-GC.o: simulator-GC.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS)  -c simulator-GC.cpp

simulator-livesize.o: simulator-livesize.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS)  -c simulator-livesize.cpp

execution.o: execution.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c execution.cpp

heap.o: heap.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS) -c heap.cpp

classinfo.o: classinfo.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c classinfo.cpp

tokenizer.o: tokenizer.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c tokenizer.cpp

memorymgr.o: memorymgr.h memorymgr.cpp heap.h
	g++ $(FLAGS) -c memorymgr.cpp

version.o: version.cpp version.hpp
	g++ $(FLAGS) -c version.cpp

clean:
	rm -f *.o simulator-GC simulator-livesize
