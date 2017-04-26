CMAKE_MINIMUM_REQUIRED(VERSION 3.3.1)
PROJECT(simulator-GC)

INCLUDE_DIRECTORIES($(simulator-GC_SOURCE_DIR))
INCLUDE_DIRECTORIES(boost_1_60_0)

EXECUTE_PROCESS( COMMAND sh ${CMAKE_SOURCE_DIR}/create-version-cpp.sh
                 OUTPUT_FILE ${CMAKE_SOURCE_DIR}/version.cpp )

#-------------------------------------------------------------------------------
# Version 1 of simulator-GC
SET(SOURCES_SIM_GC simulator-GC.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# The file(GLOB...) allows for wildcard additions if needed:
# file(GLOB SOURCES "src/*.cpp")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror")

# Main non-static simulator
ADD_EXECUTABLE(simulator-GC ${SOURCES_SIM_GC})


#-------------------------------------------------------------------------------
# Version 1 of simulator-GC
SET(SOURCES_SIM_GC_VER2 simulator-GC-ver2.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# The file(GLOB...) allows for wildcard additions if needed:
# file(GLOB SOURCES "src/*.cpp")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror")

# Main non-static simulator
ADD_EXECUTABLE(simulator-GC-ver2 ${SOURCES_SIM_GC_VER2})


# Main static simulator
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static")
ADD_EXECUTABLE(simulator-GC-ver2-static ${SOURCES_SIM_GC_VER2})

#-------------------------------------------------------------------------------
# Version 1 of simulator-GC
SET(SOURCES_SIM_PAGC_VER1 simulator-PAGC-ver1.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# Main non-static Program Aware GC simulator
ADD_EXECUTABLE(simulator-PAGC-ver1 ${SOURCES_SIM_PAGC_VER1})

