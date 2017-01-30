
# FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0 -I./TEMP/include/igraph -L./TEMP/lib -ligraph
GIT=git
FLAGS=-O2 -std=c++11 -g -Werror -I./boost_1_60_0
.PHONY: clean gitversion

all: gitversion simulator-context

gitversion:
	$(GIT) rev-parse HEAD | awk ' BEGIN {print "#include \"version.hpp\""} {print "const char *build_git_sha = \"" $$0"\";"} END {}' > version.cpp
	date | awk 'BEGIN {} {print "const char *build_git_time = \""$$0"\";"} END {} ' >> version.cpp

simulator-context: simulator-context.o execution.o heap.o classinfo.o tokenizer.o version.o\
				summary.hpp
	echo $(FLAGS)
	g++ $(FLAGS) -o simulator-context simulator-context.o execution.o heap.o classinfo.o version.o \
			tokenizer.o

# simulator-context-ver2: simulator-context-ver2.o execution.o heap.o classinfo.o tokenizer.o version.o\
# 				summary.hpp
# 	echo $(FLAGS)
# 	g++ $(FLAGS) -o simulator-context-ver2 simulator-context-ver2.o execution.o heap.o classinfo.o version.o \
# 			tokenizer.o

simulator-context.o: simulator-context.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS)  -c simulator-context.cpp

# simulator-context-ver2.o: simulator-context-ver2.cpp classinfo.h tokenizer.h heap.h
# 	g++ $(FLAGS)  -c simulator-context-ver2.cpp

execution.o: execution.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c execution.cpp

heap.o: heap.cpp classinfo.h tokenizer.h heap.h
	g++ $(FLAGS) -c heap.cpp

classinfo.o: classinfo.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c classinfo.cpp

tokenizer.o: tokenizer.cpp classinfo.h tokenizer.h
	g++ $(FLAGS) -c tokenizer.cpp

version.o: version.cpp version.hpp
	g++ $(FLAGS) -c version.cpp

clean:
	rm -f *.o simulator-context
