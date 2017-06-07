CMAKE_MINIMUM_REQUIRED(VERSION 3.3.1)
PROJECT(simulator-GC)

INCLUDE_DIRECTORIES($(simulator-GC_SOURCE_DIR))
INCLUDE_DIRECTORIES(boost_1_60_0)

EXECUTE_PROCESS( COMMAND sh ${CMAKE_SOURCE_DIR}/create-version-cpp.sh
                 OUTPUT_FILE ${CMAKE_SOURCE_DIR}/version.cpp )


SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror")

#-------------------------------------------------------------------------------
# Version 1 of simulator-GC
SET(SOURCES_SIM_PAGC_VER1 simulator-PAGC-ver1.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# Main non-static Program Aware GC simulator
ADD_EXECUTABLE(simulator-PAGC-ver1 ${SOURCES_SIM_PAGC_VER1})

#-------------------------------------------------------------------------------
# Program Aware GC MODEL Version 1
SET(SOURCES_SIM_PAGC_MODEL_1 simulator-PAGC-model-1.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# Main non-static Program Aware GC simulator
ADD_EXECUTABLE(simulator-PAGC-model-1 ${SOURCES_SIM_PAGC_MODEL_1})

#-------------------------------------------------------------------------------
# Program Aware GC FUNC MODEL Version 1
#  - based on MODEL version 1
SET(SOURCES_SIM_PAGC_FUNC_1 simulator-PAGC-func-1.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# Main non-static Program Aware GC simulator
ADD_EXECUTABLE(simulator-PAGC-func-1 ${SOURCES_SIM_PAGC_FUNC_1})

#-------------------------------------------------------------------------------
# Version 2 of simulator-GC
SET(SOURCES_SIM_PAGC_VER2 simulator-PAGC-ver2.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# Main non-static Program Aware GC simulator version 2
ADD_EXECUTABLE(simulator-PAGC-ver2 ${SOURCES_SIM_PAGC_VER2})

#-------------------------------------------------------------------------------
# Program Aware GC MODEL Version 2
SET(SOURCES_SIM_PAGC_MODEL_2 simulator-PAGC-model-2.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp memorymgr.cpp)

# Main non-static Program Aware GC simulator
ADD_EXECUTABLE(simulator-PAGC-model-2 ${SOURCES_SIM_PAGC_MODEL_2})

