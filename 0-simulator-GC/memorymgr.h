#ifndef MEMORYMGR_H
#define MEMORYMGR_H

// ----------------------------------------------------------------------
//   Representation of memory management related data structures and 
//   algorithms. This is instantiated as a part of HeapState.
//
#include <algorithm>
#include <iostream>
#include <map>
#include <deque>
#include <limits.h>
#include <assert.h>

# include "heap.h"
// #include "classinfo.h"
// #include "refstate.h"

using namespace boost;

class Region
{
public:
    // Debug flag 
    static bool debug;

    // Returns true if allocation was successful.
    //         false otherwise.
    bool allocate( Object *object,
                   unsigned int create_time );

private:
    unsigned int m_size;
};

class MemoryMgr
{
public:
    // Debug flag 
    static bool debug;

    // Returns true if allocation caused garbage collection.
    //         false otherwise.
    bool allocate( Object *object,
                   unsigned int create_time );

    MemoryMgr() {
    }


private:
};

#endif
