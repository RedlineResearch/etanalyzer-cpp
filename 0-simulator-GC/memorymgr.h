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
#include <set>
#include <string>
#include <utility>
#include <limits.h>
#include <assert.h>

// #include "classinfo.h"
// #include "refstate.h"

// using namespace boost;
using namespace std;

class Region;
class Object;

typedef map<string, Region *> RegionMap_t;
typedef set< Object * > ObjectSet_t;
typedef pair<int, int> GCRecord_t;
//      - first is timestamp, second is bytes

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
        , m_free(size)
        , m_level(level)
        , m_live_set()
        , m_garbage_waiting()
        , m_gc_history() {
    }

    // Returns true if there was space and thus successful.
    //         false otherwise.
    bool allocate( Object *object,
                   unsigned int create_time );
    // The following three functions:
    // return true if allocation was successful.
    //        false otherwise.
    bool remove( Object *object );
    bool makeDead( Object *object );
    bool add_to_garbage_set( Object *object );

    int getLevel() const  { return this->m_level; }

    int getSize() const { return m_size; }
    int getUsed() const { return m_used; }
    int getFree() const { return m_free; }
    int getLive() const { return m_live; }
    int getGarbage() const { return m_garbage; }

    deque<GCRecord_t> get_GC_history() const { return m_gc_history; }

    int collect( unsigned int timestamp );

private:
    string m_name;

    // The following 4 fields are in bytes.
    const unsigned int m_size; // Total capacity
    int m_used; // Currently in use = live + garbage
    int m_free; // free space = size - used
    int m_live; // live space (reachable, not garbage)
    int m_garbage; // garbage = in use - live

    int m_level;
    // Signifies the level in the hierarchy of regional generations.
    // Level 0 - where the memory manager allocates from
    // Level 1 - promotions from Level 0 go here.
    // ...
    // Level n - promotions from Level n-1 go here.
    ObjectSet_t m_live_set;
    ObjectSet_t m_garbage_waiting; // TODO What is m_garbage_waiting?

    // Collection history
    deque<GCRecord_t> m_gc_history;

    void addToGarbage( int add );
    int setGarbage( int newval );
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
                   unsigned int create_time,
                   unsigned int new_alloc_time );

    MemoryMgr( float GC_threshold )
        : m_region_map()
        , m_level_map()
        , m_level2name_map()
        , m_alloc_region(NULL)
        , m_GC_threshold(GC_threshold)
        , m_alloc_time(0) {
    }

    // Initializes all the regions. This should contain all knowledge
    // of how things are laid out. Virtual so you can reimplement
    // with different layouts.
    virtual bool initialize_memory( std::vector<int> sizes );

    // Get number of regions
    int numberRegions() const { return this->m_region_map.size(); }

    // Do a garbage collection
    int do_collection();

    // On a D(eath) event
    bool makeDead( Object *object, unsigned int death_time );

    // Get the GC history
    deque<GCRecord_t> get_GC_history();

private:
    // Create new region with the given name.
    // Returns a reference to the region.
    Region *new_region( string &region_name,
                        unsigned int size,
                        int level );

    // Maps from region name to Region pointer
    RegionMap_t m_region_map;
    // Maps from level to region pointer
    map< int, Region * > m_level_map;
    // Maps from level to region name
    map< int, string > m_level2name_map;
    // The designated allocation region (ie level 0)
    Region *m_alloc_region; // TODO Maybe this can be an index?
    // Garbage collection threshold if used by collector.
    // In the base class configuration with 1 region, this is the
    // heap occupancy percentage threshold that triggers garbage
    // collection.
    // If more complex generational/region types of memory management is
    // needed, then 'initialize_memory' needs to be overridden in the 
    // inheriting class.
    float m_GC_threshold;
    // Logical allocation time
    unsigned int m_alloc_time;
};

#endif
