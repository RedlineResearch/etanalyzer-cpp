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
#include <vector>
#include <string>
#include <limits.h>
#include <assert.h>

// #include "classinfo.h"
// #include "refstate.h"

// using namespace boost;
using namespace std;

class Region;
class Object;

typedef map<string, Region *> RegionMap;


class Region
{
public:
    // Debug flag 
    static bool debug;

    // Constructor
    Region( string &name,
            unsigned int size,
            int level )
        : m_name(name)
        , m_size(size)
        , m_level(level) {
    }
    // Returns true if allocation was successful.
    //         false otherwise.
    bool allocate( Object *object,
                   unsigned int create_time );

    int getLevel() const  { return this->m_level; }

private:
    string m_name;
    unsigned int m_size;

    int m_level;
    // Signifies the level in the hierarchy of regional generations.
    // Level 0 - where the memory manager allocates from
    // Level 1 - promotions from Level 0 go here.
    // ...
    // Level n - promotions from Level n-1 go here.
};

class MemoryMgr
{
public:
    // Debug flag 
    static bool debug;
    // Allocation region name. Users of MemoryMgr should create
    // one region with this name. All allocations will go to this region.
    static string ALLOC;

    // Returns true if allocation caused garbage collection.
    //         false otherwise.
    bool allocate( Object *object,
                   unsigned int create_time );

    MemoryMgr( float gc_threshold )
        : m_region_map()
        , m_level_map()
        , m_level2name_map()
        , m_alloc_region(NULL)
        , m_gc_threshold(gc_threshold) {
    }

    // Initializes all the regions. This should contain all knowledge
    // of how things are laid out. Virtual so you can reimplement
    // with different layouts.
    virtual bool initialize_memory( std::vector<int> sizes );

    // Get number of regions
    int numberRegions() const { return this->m_region_map.size(); }

private:
    // Create new region with the given name.
    // Returns a reference to the region.
    Region &new_region( string &region_name,
                        unsigned int size,
                        int level );

    // Maps from region name to Region pointer
    RegionMap m_region_map;
    // Maps from level to region pointer
    map< int, Region * > m_level_map;
    // Maps from level to region name
    map< int, string > m_level2name_map;
    // The designated allocation region (ie level 0)
    Region *m_alloc_region;
    // Garbage collection threshold if used by collector.
    // In the base class configuration with 1 region, this is the
    // heap occupancy percentage threshold that triggers garbage
    // collection.
    // If more complex generational/region types of memory management is
    // needed, then 'initialize_memory' needs to be overridden in the 
    // inheriting class.
    float m_gc_threshold;
};

#endif
