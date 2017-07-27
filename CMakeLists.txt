CMAKE_MINIMUM_REQUIRED(VERSION 3.3.1)
PROJECT(etanalyzer-cpp)

# Bring the headers. Is this needed only if they are in a different directory?
# include_directories(include)
# FIND_PACKAGE( Boost REQUIRED )
# ADD_LIBRARY(boost INTERFACE IMPORTED)
# SET_PROPERTY(TARGET boost PROPERTY
#              INTERFACE_INCLUDE_DIRECTORIES ${Boost_INCLUDE_DIR})

INCLUDE_DIRECTORIES($(etanalyzer-cpp_SOURCE_DIR))
INCLUDE_DIRECTORIES(boost_1_60_0)

ADD_SUBDIRECTORY(EASTL)
INCLUDE_DIRECTORIES(EASTL/include)
INCLUDE_DIRECTORIES(EASTL/test/packages/EABase/include/Common)
TARGET_LINK_LIBRARIES(EASTL)

# Set the custom directory for GMP
INCLUDE_DIRECTORIES(GMP/include)
LINK_DIRECTORIES(GMP/lib)

EXECUTE_PROCESS( COMMAND sh ${CMAKE_SOURCE_DIR}/create-version-cpp.sh
                 OUTPUT_FILE ${CMAKE_SOURCE_DIR}/version.cpp )

SET(SOURCES_SIM simulator.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
    version.cpp)

# TODO SET(SOURCES_SIM_POOL simulator-pool.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
# TODO     version.cpp)
# TODO 
# TODO SET(SOURCES_SIM_FIND_MAIN simulator-find-main.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
# TODO     version.cpp)
# TODO 
# TODO SET(SOURCES_SIM_REFCOUNT simulator-ref.cpp execution.cpp heap.cpp classinfo.cpp tokenizer.cpp
# TODO     version.cpp)
# TODO 
# TODO SET(SOURCES_SIM_DB simulator-DB.cpp execution.cpp heap-DB.cpp classinfo.cpp tokenizer.cpp
# TODO     version.cpp)

# The file(GLOB...) allows for wildcard additions if needed:
# file(GLOB SOURCES "src/*.cpp")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -std=c++11 -g -Werror")

# Main static simulator
ADD_EXECUTABLE(simulator ${SOURCES_SIM})
TARGET_LINK_LIBRARIES(simulator gmp)
# TARGET_COMPILE_DEFINITIONS(simulator
#                            PRIVATE _SIZE_DEBUG)

# TODO # Main static simulator
# TODO ADD_EXECUTABLE(simulator-pool ${SOURCES_SIM_POOL})

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
TARGET_LINK_LIBRARIES(simulator-type1 gmp)

# TODO ADD_EXECUTABLE(simulator-find-main ${SOURCES_SIM_FIND_MAIN})

# DEBUG simulator executable
ADD_EXECUTABLE(simulator-debug ${SOURCES_SIM})
TARGET_COMPILE_DEFINITIONS(simulator-debug
                           PRIVATE DEBUG_SPECJBB)
TARGET_LINK_LIBRARIES(simulator-debug gmp)

# Ref counting simulator executable
# TODO: ADD_EXECUTABLE(simulator-refcount ${SOURCES_SIM_REFCOUNT})

# TODO # Simulator using an out of memory (on-disk) db to reduce memory usage
# TODO ADD_EXECUTABLE(simulator-DB ${SOURCES_SIM_DB})
