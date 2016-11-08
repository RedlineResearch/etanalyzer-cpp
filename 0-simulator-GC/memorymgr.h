#ifndef MEMORYMGR_H
#define MEMORYMGR_H

// ----------------------------------------------------------------------
//   Representation of memory management related data structures and 
//   algorithms. This is instantiated as a part of HeapState.
//
#include <algorithm>
#include <iostream>
#include <fstream>
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

typedef unsigned int ObjectId_t;

typedef std::map<string, Region *> RegionMap_t;
typedef std::set< Object * > ObjectSet_t;
typedef std::set< ObjectId_t > ObjectIdSet_t;
typedef pair<int, int> GCRecord_t;
//      - first is timestamp, second is bytes

// If doing sets of things that aren't primitives, then you need
// to supply a comparator class to the set definition.
struct _compclass {
    bool operator() ( const std::pair< ObjectId_t, unsigned int >& lhs,
                      const std::pair< ObjectId_t, unsigned int >& rhs ) const {
        return lhs.second > rhs.second;
    }
};

typedef unsigned int EdgeId_t;
// A pair of edge Ids
typedef std::pair< ObjectId_t, ObjectId_t > EdgeIdPair_t;
// A set of Edge pairs
typedef std::set< EdgeIdPair_t, _compclass > ObjectIdPairSet_t;
// A map:
// key: edge id
//     -> val: edge id set
typedef std::map< ObjectId_t, ObjectIdSet_t > ObjectId2SetMap_t;


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
        , m_used(0)
        , m_live(0)
        , m_garbage(0)
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
    // bool remove( Object *object );
    bool makeDead( Object *object );
    void add_to_garbage_set( Object *object );

    int getLevel() const  { return this->m_level; }

    int getSize() const { return m_size; }
    int getUsed() const { return m_used; }
    int getFree() const { return m_free; }
    int getLive() const { return m_live; }
    int getGarbage() const { return m_garbage; }
    unsigned int long get_num_GC_attempts() const { return this->GC_attempts; }

    deque<GCRecord_t> get_GC_history() const { return m_gc_history; }

    int collect( unsigned int timestamp );

    // Debug functions
    void print_status();

private:
    string m_name;

    // The following 4 fields are in bytes.
    const unsigned int m_size; // Total capacity
    int m_used; // Currently in use = live + garbage
    int m_free; // free space = size - used
    int m_live; // live space (reachable, not garbage)
    int m_garbage; // garbage = in use - live
    // => also the total size in bytes of all objects in m_garbage_waiting set

    int m_level;
    // Signifies the level in the hierarchy of regional generations.
    // Level 0 - where the memory manager allocates from
    // Level 1 - promotions from Level 0 go here.
    // ...
    // Level n - promotions from Level n-1 go here.
    ObjectSet_t m_garbage_waiting; // TODO What is m_garbage_waiting?
    ObjectSet_t m_live_set;

    // Collection history
    deque<GCRecord_t> m_gc_history;
    unsigned long int GC_attempts;

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
        , m_live_set()
        , m_specgroup()
        , m_region_edges()
        , m_nonregion_edges()
        , m_in_edges()
        , m_out_edges()
        , m_srcidmap()
        , m_tgtidmap()
        , m_alloc_region(NULL)
        , m_times_GC(0)
        , m_GC_threshold(GC_threshold)
        , m_alloc_time(0)
        , m_edges_removed(0) {
    }

    // Initializes all the regions. This should contain all knowledge
    // of how things are laid out. Virtual so you can reimplement
    // with different layouts.
    virtual bool initialize_memory( std::vector<int> sizes );

    // Initialize the grouped region of objects
    virtual void initialize_special_group( string &group_filename,
                                           int numgroups );

    // Get number of regions
    int numberRegions() const { return this->m_region_map.size(); }

    // TODO // Do a garbage collection
    // TODO int do_collection();

    // Do a garbage collection only if needed.
    bool should_do_collection();

    // On a D(eath) event
    bool makeDead( Object *object, unsigned int death_time );

    // On an U(pdate) event
    void add_edge( ObjectId_t src, ObjectId_t tgt );
    void remove_edge( ObjectId_t src, ObjectId_t oldTgtId );
    void remove_object( ObjectId_t objId );

    void remove_from_srcidmap( ObjectId_t src,
                               ObjectId_t oldTgtId );
    void remove_from_tgtidmap( ObjectId_t src,
                               ObjectId_t tgtId );

    // Get the GC history
    deque<GCRecord_t> get_GC_history();

    // Check if object is in live set
    bool is_in_live_set( Object *object );

    // Return the live size total in bytes
    unsigned long int getLiveSize() const { return this->m_liveSize; }
    // Return the current maximum live size total in bytes
    unsigned long int getMaxLiveSize() const { return this->m_maxLiveSize; }
    // Get total size capacity in bytes
    unsigned long int getTotalSize() const { return this->m_total_size; }

    // Debug functions
    void print_status();
    unsigned long int get_num_GC_attempts( bool printflag );

private:
    // Total size being managed
    unsigned long int m_total_size;
    // Total number of collections done
    unsigned int m_times_GC;

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
    // GC threshold in bytes
    unsigned long int m_GC_byte_threshold;
    // Logical allocation time
    unsigned int m_alloc_time;
    // MemoryMgr is expected to keep track of objects so that we can handle
    // duplicate allocations properly
    ObjectSet_t m_live_set;
    // The special group of objects that we are to ignore during collections
    ObjectIdSet_t m_specgroup;
    // Number of groups
    int m_numgroups;
    // NOTE: This is temporarily at 1 for experimental purposes.
    //       If ever there's a need for more groups, then the following
    //       need to be changed:
    // 1. m_specgroup - There will be more than one group, so this has to be a
    //                  map from group Id to the set comprising the actual group.
    // 2. All the set of edges
    //     - m_region_edges
    //     - m_in_edges
    //     - m_out_edges
    //     - m_nonregion_edges
    //    Note that this may get exponentially more complicated as keeping track
    //    of edges between groups will be complicated under the current scheme.
    //    - RLV 7 Nov 2016
    
    // Live size should be here because this is where the live set it managed.
    unsigned long int m_liveSize; // current live size of program heap in bytes
    unsigned long int m_maxLiveSize; // current maximum live size of program heap in bytes

    // Edge sets and remember sets
    //     * edges where source and target are in the region
    ObjectIdPairSet_t m_region_edges;
    //     * edges where source is outside and target is in the region
    ObjectIdPairSet_t m_in_edges;
    //     * edges where source is inside and target is outside the region
    ObjectIdPairSet_t m_out_edges;
    //     * edges where both source and target are outside the region
    ObjectIdPairSet_t m_nonregion_edges;

    // Src id to set of tgt ids
    ObjectId2SetMap_t m_srcidmap;
    // Tgt id to set of src ids
    ObjectId2SetMap_t m_tgtidmap;

    // Debugging GC
    unsigned long int GC_attempts;
    int m_edges_removed;
};

#endif
