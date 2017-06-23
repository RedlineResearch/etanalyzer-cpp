CMAKE_MINIMUM_REQUIRED(VERSION 3.3.1)
PROJECT(cycle-analyzer)

ADD_SUBDIRECTORY(EASTL)
INCLUDE_DIRECTORIES(EASTL/include)
INCLUDE_DIRECTORIES(EASTL/test/packages/EABase/include/Common)
TARGET_LINK_LIBRARIES(EASTL)

INCLUDE_DIRECTORIES($(cycle-analyzer_SOURCE_DIR))
# TODO INCLUDE_DIRECTORIES(boost_1_60_0)

# NOTE: CMAKE_SOURCE_DIR is the oribinal source directory
EXECUTE_PROCESS( COMMAND sh ${CMAKE_SOURCE_DIR}/create-version-cpp.sh
                 OUTPUT_FILE ${CMAKE_SOURCE_DIR}/version.cpp )

SET( SOURCES_SIM simulator-ver2.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
     version.cpp )

# The file(GLOB...) allows for wildcard additions if needed:
# file(GLOB SOURCES "src/*.cpp")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror")

# Main simulator
ADD_EXECUTABLE( simulator-ver2 ${SOURCES_SIM} )
# TARGET_COMPILE_DEFINITIONS(simulator
#                            PRIVATE _SIZE_DEBUG)

# Non-static simulator
# TODO: Add a build option to enable static building when needed
#       Need to change the SET to the TARGET_COMPILE_OPTIONS
# SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror -static")
# ADD_EXECUTABLE(simulator-type1 ${SOURCES_SIM})
# TARGET_COMPILE_OPTIONS(simulator-dyn
#                        PRIVATE static)
