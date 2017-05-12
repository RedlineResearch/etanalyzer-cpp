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

typedef unsigned int EdgeId_t;
// A pair of edge Ids
typedef std::pair< ObjectId_t, ObjectId_t > ObjectIdPair_t;

// TODO DELETE: // If doing sets of things that aren't primitives, then you need
// TODO DELETE: // to supply a comparator class to the set definition.
// TODO DELETE: struct _compclass {
// TODO DELETE:     bool operator() ( const ObjectIdPair_t &lhs,
// TODO DELETE:                       const ObjectIdPair_t &rhs ) const {
// TODO DELETE:         if (lhs.first == rhs.first) {
// TODO DELETE:             return lhs.second < rhs.second;
// TODO DELETE:         } else {
// TODO DELETE:             return lhs.first < rhs.first;
// TODO DELETE:         }
// TODO DELETE:     }
// TODO DELETE: };

// A set of Edge pairs
// TODO: TO DELETE
// Using a set with a pair wasn't working and I don't have time to 
// debug. So I'm just going to use a map to a set.
// TODO: DELETE typedef std::set< ObjectIdPair_t, _compclass > ObjectIdPairSet_t;
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
            int level )
        : m_name(name)
        , m_live(0)
        , m_garbage(0)
        , m_level(level)
        , m_live_set()
        // TODO , m_region_edges()
        // TODO , m_in_edges()
        // TODO , m_out_edges()
        , m_dtime(0)
        , m_garbage_waiting()
        , m_gc_history() {
    }

    ~Region() {
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

    int collect( unsigned int timestamp, unsigned int timestamp_alloc );

    //----------------------------------------------------------------------
    int getLevel() const  { return this->m_level; }

    int getLive() const { return m_live; }
    int getGarbage() const { return m_garbage; }
    unsigned int long get_num_GC_attempts() const {
        return this->GC_attempts;
    }

    void set_region_deathtime( unsigned int dtime ) {
        this->m_dtime = dtime;
    }

    unsigned int get_region_deathtime() {
        return this->m_dtime;
    }

    deque<GCRecord_t> get_GC_history() const { return this->m_gc_history; }

    // Get region name
    string get_name() const { return this->m_name; }

    // Debug functions
    void print_status();

protected:
    // TODO // Edge sets and remember sets
    // TODO //     * edges where source and target are in the region
    // TODO ObjectId2SetMap_t m_region_edges;
    // TODO //     * edges where source is outside and target is in the region
    // TODO ObjectId2SetMap_t m_in_edges;
    // TODO //     * edges where source is inside and target is outside the region
    // TODO ObjectId2SetMap_t m_out_edges;

    // Expected death time in allocation byte time
    unsigned int m_dtime;

private:
    string m_name;

    void addToGarbage( int add );
    int setGarbage( int newval );

    // The following fields are in bytes.
    int m_live; // live space (reachable, not garbage)
    int m_garbage; // garbage

    int m_number_of_collections;

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

};

// TODO DELETE
// enum class ManagerType {
//     Simple,
//     Deferred,
//     Undefined
// };

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
    virtual bool allocate( Object *object,
                           unsigned int create_time,
                           unsigned int new_alloc_time );

    MemoryMgr()
        : m_region_map()
        , m_level_map()
        , m_level2name_map()
        , m_live_set()
        , m_free(0)
        , m_used(0)
        , m_liveSize(0)
        , m_garbage(0)
        , m_total_size(0)
        , m_mark_nonregion_total(0)
        , m_specgroup()
        , m_nonregion_edges()
        , m_srcidmap()
        , m_tgtidmap()
        , m_alloc_region(NULL)
        , m_times_GC(0)
        // TODO , m_GC_threshold(GC_threshold)
        , m_alloc_time(0)
        , m_attempts_edges_removed(0)
        , m_edges_removed(0) {
    }

    ~MemoryMgr() {
    }

    // Initializes all the regions. This should contain all knowledge
    // of how things are laid out. Virtual so you can reimplement
    // with different layouts.
    virtual bool initialize_memory( unsigned long int heapsize );

    // Initialize the grouped region of objects
    virtual bool initialize_special_group( string &group_filename,
                                           int numgroups ) {
        // DO NOTHING.
        // There's no special gropu in the BASIC MemoryMgr.
    }

    // Get number of regions
    int numberRegions() const { return this->m_region_map.size(); }

    // TODO // Do a garbage collection
    // TODO int do_collection();

    int get_number_of_collections() const { return this->m_times_GC; }


    // Do a garbage collection only if needed.
    virtual bool should_do_collection();

    // On a D(eath) event
    virtual bool makeDead( Object *object, unsigned int death_time );

    // On an U(pdate) event
    virtual void add_edge( ObjectId_t src, ObjectId_t tgt );
    virtual void remove_edge( ObjectId_t src, ObjectId_t oldTgtId );
    virtual void remove_object( ObjectId_t objId );

    void remove_from_srcidmap( ObjectId_t src,
                               ObjectId_t oldTgtId );
    void remove_from_tgtidmap( ObjectId_t src,
                               ObjectId_t tgtId );

    // Check if object is in live set
    virtual bool is_in_live_set( Object *object );

    // Return the live size total in bytes
    unsigned long int getLiveSize() const { return this->m_liveSize; }
    // Return the current maximum live size total in bytes
    unsigned long int getMaxLiveSize() const { return this->m_maxLiveSize; }
    // Get total size capacity in bytes
    unsigned long int getTotalSize() const { return this->m_total_size; }

    //----------------------------------------------------------------------
    // Debug functions
    //
    // Get the GC history
    deque<GCRecord_t> get_GC_history();
    unsigned long int get_num_GC_attempts( bool printflag );

    // - TODO Documentation
    // Getter functions for getting the number of edges
    int get_number_edges_removed() const { return this->m_edges_removed; }
    int get_number_attempts_edges_removed() const { return this->m_attempts_edges_removed; }
    unsigned int get_region_edges_count() const { return this->m_region_edges_count; }
    unsigned int get_in_edges_count() const { return this->m_in_edges_count; }
    unsigned int get_out_edges_count() const { return this->m_out_edges_count; }
    unsigned int get_nonregion_edges_count() const { return this->m_nonregion_edges_count; }

    // Mark related getters
    virtual unsigned int get_mark_total() const {
        return this->m_mark_nonregion_total;
    }

    virtual unsigned int get_mark_saved() const
    {
        return this->m_mark_saved_total;
    }

    virtual unsigned int get_mark_nonregion_total() const {
        return this->m_mark_nonregion_total;
    }
    //
    // These return 0 in the basic MemoryMgr because we don't use them.
    virtual unsigned int get_mark_saved_total() const {
        return 0;
    }

    virtual unsigned int get_mark_region_total() const {
        return 0;
    }


    // - TODO Documentation
    void print_status();

protected:
    // Total size being managed
    unsigned long int m_total_size;
    // Total number of collections done
    unsigned int m_times_GC;
    // Mark totals
    unsigned int m_mark_nonregion_total;
    unsigned int m_mark_saved_total;
    unsigned int m_mark_region_total;

    // Create new region with the given name.
    // Returns a reference to the region.
    Region *new_region( string &region_name,
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
    // GC threshold in bytes
    unsigned long int m_GC_byte_threshold;
    // TODO DELETE float m_GC_threshold;

    // Logical allocation time
    unsigned int m_alloc_time;
    // MemoryMgr is expected to keep track of objects so that we can handle
    // duplicate allocations properly
    ObjectSet_t m_live_set;

    // The special group of objects that we are to ignore during collections
    ObjectIdSet_t m_specgroup;
    // Number of groups
    int m_numgroups;
    
    // Space related variables (in bytes)
    // These should be here because this is where the live set is managed.
    unsigned long int m_size; // How much memory total
    unsigned long int m_free; // How much free space = size - used
    unsigned long int m_used; // How much is used = live + garbage
    unsigned long int m_garbage; // How much is used but unreachable
    unsigned long int m_liveSize; // current live size of program heap in bytes
    unsigned long int m_maxLiveSize; // current maximum live size of program heap in bytes

    // Edge set
    //     * edges where both source and target are outside the region
    ObjectId2SetMap_t m_nonregion_edges;
    // Well for the base MemoryMgr, this is the only edge set.

    // Counts of total edges added
    unsigned int m_region_edges_count;
    unsigned int m_in_edges_count;
    unsigned int m_out_edges_count;
    unsigned int m_nonregion_edges_count;

    // Src id to set of tgt ids
    ObjectId2SetMap_t m_srcidmap;
    // Tgt id to set of src ids
    ObjectId2SetMap_t m_tgtidmap;

    // Debugging GC
    unsigned long int GC_attempts;
    int m_edges_removed;
    int m_attempts_edges_removed;
};

class MemoryMgrDef : public MemoryMgr
{
private:
    // static string ALLOC;
    static string SPECIAL;

public:
    MemoryMgrDef()
        : m_region_edges_p( NULL )
        , m_in_edges_p( NULL )
        , m_out_edges_p( NULL )
        , m_defregion_p( NULL )
        , MemoryMgr() {
        }
    // Returns true if allocation caused garbage collection.
    //         false otherwise.
    virtual bool allocate( Object *object,
                           unsigned int create_time,
                           unsigned int new_alloc_time );

    // On a D(eath) event
    virtual bool makeDead( Object *object, unsigned int death_time );

    // Edges (adding and removing)
    virtual void add_edge( ObjectId_t src, ObjectId_t tgt );
    virtual void remove_edge( ObjectId_t src, ObjectId_t oldTgtId );
    //
    // Initializes all the regions. This should contain all knowledge
    // of how things are laid out.
    virtual bool initialize_memory( unsigned long int heapsize );

    // Initialize the grouped region of objects
    virtual bool initialize_special_group( string &group_filename,
                                           int numgroups );

    // Mark count getter functions
    // Mark related getters
    virtual unsigned int get_mark_saved_total() const
    {
        return this->m_mark_saved_total;
    }

    virtual unsigned int get_mark_region_total() const
    {
        return this->m_mark_region_total;
    }

protected:
    // Edge sets and remember sets
    //     * edges where source and target are in the region
    ObjectId2SetMap_t *m_region_edges_p;
    //     * edges where source is outside and target is in the region
    ObjectId2SetMap_t *m_in_edges_p;
    //     * edges where source is inside and target is outside the region
    ObjectId2SetMap_t *m_out_edges_p;

    Region *m_defregion_p;

};

class MemoryMgrDefVer2 : public MemoryMgr
{
private:
    // static string ALLOC;
    static string SPECIAL_VER2;

public:
    MemoryMgrDefVer2()
        : m_region_edges_p( NULL )
        , m_in_edges_p( NULL )
        , m_out_edges_p( NULL )
        , m_defregion_p( NULL )
        , MemoryMgr() {
        }
    // Returns true if allocation caused garbage collection.
    //         false otherwise.
    virtual bool allocate( Object *object,
                           unsigned int create_time,
                           unsigned int new_alloc_time );

    // On a D(eath) event
    virtual bool makeDead( Object *object, unsigned int death_time );

    // Edges (adding and removing)
    virtual void add_edge( ObjectId_t src, ObjectId_t tgt );
    virtual void remove_edge( ObjectId_t src, ObjectId_t oldTgtId );
    //
    // Initializes all the regions. This should contain all knowledge
    // of how things are laid out.
    virtual bool initialize_memory( unsigned long int heapsize );

    // Initialize the grouped region of objects
    virtual bool initialize_special_group( string &group_filename,
                                           int numgroups );

    // Mark count getter functions
    // Mark related getters
    virtual unsigned int get_mark_saved_total() const
    {
        return this->m_mark_saved_total;
    }

    virtual unsigned int get_mark_region_total() const
    {
        return this->m_mark_region_total;
    }

protected:
    // Edge sets and remember sets
    //     * edges where source and target are in the region
    ObjectId2SetMap_t *m_region_edges_p;
    //     * edges where source is outside and target is in the region
    ObjectId2SetMap_t *m_in_edges_p;
    //     * edges where source is inside and target is outside the region
    ObjectId2SetMap_t *m_out_edges_p;

    Region *m_defregion_p;

};

#endif
