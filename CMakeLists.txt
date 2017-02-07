CMAKE_MINIMUM_REQUIRED(VERSION 3.3.1)
PROJECT(etanalyzer-cpp)

# Bring the headers. Is this needed only if they are in a different directory?
# include_directories(include)
FIND_PACKAGE(Boost REQUIRED)
ADD_LIBRARY(boost INTERFACE IMPORTED)
SET_PROPERTY(TARGET boost PROPERTY
             INTERFACE_INCLUDE_DIRECTORIES ${Boost_INCLUDE_DIR})

ADD_SUBDIRECTORY(EASTL)
INCLUDE_DIRECTORIES(EASTL/include)
INCLUDE_DIRECTORIES(EASTL/test/packages/EABase/include/Common)
TARGET_LINK_LIBRARIES(EASTL)

SET(SOURCES_SIM simulator.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    analyze.cpp version.cpp)

SET(SOURCES_SIM_FIND_MAIN simulator-find-main.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    analyze.cpp version.cpp)

EXECUTE_PROCESS( COMMAND sh ${CMAKE_SOURCE_DIR}/create-version-cpp.sh
                 OUTPUT_FILE version.cpp )

# The file(GLOB...) allows for wildcard additions if needed:
# file(GLOB SOURCES "src/*.cpp")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror")

# Main static simulator
ADD_EXECUTABLE(simulator ${SOURCES_SIM})

# Non-static simulator
# TODO: Add a build option to enable static building when needed
#       Need to change the SET to the TARGET_COMPILE_OPTIONS
# SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror -static")
# ADD_EXECUTABLE(simulator-type1 ${SOURCES_SIM})
# TARGET_COMPILE_OPTIONS(simulator-dyn
#                        PRIVATE static)

ADD_EXECUTABLE(simulator-type1 ${SOURCES_SIM})
TARGET_COMPILE_DEFINITIONS(simulator-type1 
                           PRIVATE ENABLE_TYPE1)

ADD_EXECUTABLE(simulator-find-main ${SOURCES_SIM_FIND_MAIN})
