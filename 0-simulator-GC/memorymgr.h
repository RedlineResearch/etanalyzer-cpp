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

class Region;

typedef map<string, Region *> RegionMap;


class Region
{
public:
    // Debug flag 
    static bool debug;

    // Constructor
    Region( string &name,
            unsigned int size )
        : m_name(name)
        , m_size(size) {
    }
    // Returns true if allocation was successful.
    //         false otherwise.
    bool allocate( Object *object,
                   unsigned int create_time );

private:
    string m_name;
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

    // Create new region with the given name.
    // Returns a reference to the region.
    Region &new_region( string &region_name,
                        unsigned int size );

    MemoryMgr()
        : m_region_map() {
    }


private:
    RegionMap m_region_map;
};

#endif
