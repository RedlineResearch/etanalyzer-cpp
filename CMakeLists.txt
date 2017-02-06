CMAKE_MINIMUM_REQUIRED(VERSION 3.3.1)
PROJECT(etanalyzer-cpp)

# Bring the headers. Is this needed only if they are in a different directory?
# include_directories(include)

SET(SOURCES_SIM simulator.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    analyze.cpp version.cpp)

SET(SOURCES_SIM_TYPE1 simulator.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    analyze.cpp version.cpp)

EXECUTE_PROCESS( COMMAND sh ${CMAKE_SOURCE_DIR}/create-version-cpp.sh
                 OUTPUT_FILE version.cpp )

# The file(GLOB...) allows for wildcard additions if needed:
# file(GLOB SOURCES "src/*.cpp")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror -static ")
ADD_EXECUTABLE(simulator ${SOURCES_SIM})

ADD_EXECUTABLE(simulator-type1 ${SOURCES_SIM_TYPE1})
TARGET_COMPILE_DEFINITIONS(simulator-type1 
                           PRIVATE ENABLE_TYPE1)
