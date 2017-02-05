CMAKE_MINIMUM_REQUIRED(VERSION 3.3.1)
PROJECT(etanalyzer-cpp)

# Bring the headers. Is this needed only if they are in a different directory?
# include_directories(include)

# SET(SOURCES simulator.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
#     analyze.cpp version.cpp)

MESSAGE( STATUS ${CMAKE_SOURCE_DIR} )
EXECUTE_PROCESS( COMMAND sh ${CMAKE_SOURCE_DIR}/create-version-cpp.sh
                 OUTPUT_FILE version.cpp )

# The file(GLOB...) allows for wildcard additions if needed:
# file(GLOB SOURCES "src/*.cpp")

# ADD_EXECUTABLE(simulator ${SOURCES})


# SAVE CODE:
#                  COMMAND awk "BEGIN {print \"#include \\\"version.hpp\\\"\"} {print \"const char *build_git_sha = \\\"\"$0\"\\\";\"} END {}"
#                  COMMAND date
#                  COMMAND awk "BEGIN {print \"const char *build_git_time = \\\"\"$0\"\\\";\"} END {}"
