#include "memorymgr.h"
#include "heap.h"

#include <utility>

// -- Global flags
bool MemoryMgr::debug = false;
string MemoryMgr::ALLOC = "ALLOC";
bool Region::debug = false;

// TODO using namespace boost;

//---------------------------------------------------------------------------------
//===========[ Region ]============================================================

// Returns true if allocation was successful.
//         false otherwise.
bool Region::allocate( Object *object,
                       unsigned int create_time )
{
    // Check to see if there's space
    unsigned int objSize = object->getSize();
    if (objSize > this->m_free) {
        return false;
    }
    // Duplicate allocates are a problem.
    // We do this check in the MemoryMgr as the object MAY have
    // been allocated in a different region.
    this->m_live_set.insert( object );
    this->m_free -= objSize; // Free goes down.
    this->m_used += objSize; // Used goes up.

    return true;
    // TODO: do i need create_time? And if yes, how do I save it?
}

bool Region::remove( Object *object )
{
    ObjectSet_t::iterator iter = this->m_live_set.find(object);
    if (iter == this->m_live_set.end()) {
        return false;
    }
    unsigned int objSize = object->getSize();
    this->m_live_set.erase(iter);
    this->m_free += objSize; // Free goes up.
    this->m_used -= objSize; // Used goes down.
    this->m_live -= objSize; // Live goes down.
    assert(this->m_free <= this->m_size);
    assert(this->m_used >= 0);
    return true;
}

void Region::add_to_garbage_set( Object *object )
{
    unsigned int objSize = object->getSize();
    // Livesize goes down.
    this->m_live -= objSize;
    // Add to garbage waiting set
    this->m_garbage_waiting.insert(object);
    // Keep a running total of how much garbage there is.
    this->addToGarbage( objSize );
    // Set the flag. This is the ONLY place this flag is set.
    object->setGarbageFlag();
}

bool Region::makeDead( Object *object )
{
    // Found object.
    // Remove from m_live_set and put into m_garbage_waiting
    bool flag = false;
    ObjectSet_t::iterator iter = this->m_live_set.find(object);
    if (iter == this->m_live_set.end()) {
        // Not in live set.
        cerr << "ERROR: makeDead on object Id[ " << object->getId()
             << " ] but can not find in live set.";
        return false;
    }
    if (!object->isGarbage()) {
        this->add_to_garbage_set( object );
        // TODO: Anything else I need to do here?
        // If flag (result) is false, that means I tried to add to 
        // the garbage set but didn't find it there.
    } else {
        // What do we do if the object was already garbage?
        cerr << "ERROR: makeDead on object Id[ " << object->getId()
             << " ] already set to garbage.";
    }
    // Remove from live_set regardless.
    this->m_live_set.erase(iter);
    // Note that we don't adjust the sizes here. This is done in
    // 'add_to_garbage_set'.
    // return whether or not we were able to make the object garbage.
    return flag;
}

int Region::collect( unsigned int timestamp )
{
    // Clear the garbage waiting set and return the space to free.
    int collected = this->m_garbage;

    this->m_garbage_waiting.clear();
    // this->m_garbage = 0;
    this->setGarbage(0);
    cout << "GC[ " << timestamp << ", " << collected << "]" << endl;
    this->m_free += collected;
    GCRecord_t rec = make_pair( timestamp, collected );
    this->m_gc_history.push_back( rec );
    return collected;
}

inline void Region::addToGarbage( int add )
{
    this->m_garbage += add;
    // DEBUG cout << "ADD: " << this->m_garbage << endl;
}

int Region::setGarbage( int newval )
{
    this->m_garbage = newval;
    // DEBUG cout << "SET: " << this->m_garbage << endl;
    return newval;
}

//---------------------------------------------------------------------------------
//===========[ MemoryMgr ]=========================================================

// Initialize all the memory and regions
// Takes a std::vector list of sizes.
// Assuming index of size corresponds to level
bool MemoryMgr::initialize_memory( vector<int> sizes )
{
    int level = 0;
    // This needs fixing: TODO
    // Do I send in a vector of NAMES for the regions?
    assert(sizes.size() == 1); // This is a single region collector.
    vector<int>::iterator iter = sizes.begin();
    this->m_alloc_region = this->new_region( MemoryMgr::ALLOC,
                                             *iter,
                                             level ); // Level 0 is required.
    ++iter;
    ++level;
    string myname("OTHER");
    while (iter != sizes.end()) {
        this->new_region( myname,
                          *iter,
                          level );
        ++iter;
        ++level;
    }
    return true;
}

// Do a garbage collection
// Returns number of bytes collected
int MemoryMgr::do_collection()
{
    return 0;
}


// Returns true if allocation caused garbage collection.
//         false otherwise.
bool MemoryMgr::allocate( Object *object,
                          unsigned int create_time,
                          unsigned int new_alloc_time )
{
    assert(this->m_alloc_region);
    this->m_alloc_time = new_alloc_time;
    ObjectSet_t::iterator iter = this->m_live_set.find( object );
    if (iter != this->m_live_set.end()) {
        // Found a dupe.
        // Always return true, but ignore the actual allocation.
        return true;
    }
    // Decisions for collection should be done here at the MemoryMgr level.
    bool done = this->m_alloc_region->allocate( object, create_time );
    if (!done) {
        // Not enough free space.
        int collected = this->m_alloc_region->collect( create_time );
        // Try again. Note that we only try one more time as the 
        // basic collector will give back all possible free memory.
        done = this->m_alloc_region->allocate( object, create_time );
        // NOTE: In a setup with more than one region, the MemoryMgr could
        // go through all regions trying to find free space. And returning
        // 'false' means an Out Of Memory Error (OOM).
    }
    return done;
}

// Create new region with the given name.
// Returns a reference to the region.
Region *MemoryMgr::new_region( string &region_name,
                               unsigned int region_size,
                               int level )
{
    RegionMap_t::iterator iter = this->m_region_map.find(region_name);
    // Blow up if we create a new region with the same name.
    assert(iter == this->m_region_map.end());
    assert(level >= 0); // TODO make this more informative
    Region *regptr = new Region( region_name, region_size, level );
    assert(regptr); // TODO make this more informative
    this->m_region_map[region_name] = regptr;
    return regptr;
}

bool MemoryMgr::makeDead( Object *object, unsigned int death_time )
{
    // Which region? Since we only have one region in this basic MemmoryMgr:
    this->m_alloc_region->makeDead( object );
    object->makeDead( death_time, this->m_alloc_time );
    // TODO: Think about whether calling object->makeDead is better in region::makeDead
    return true;
}

deque<GCRecord_t> MemoryMgr::get_GC_history()
{
    deque<GCRecord_t> result;
    for ( RegionMap_t::iterator iter = this->m_region_map.begin();
          iter != this->m_region_map.end();
          ++iter ) {
        Region *ptr = iter->second;
        deque<GCRecord_t> myhist = ptr->get_GC_history();
        result.insert( result.end(), myhist.begin(), myhist.end() );
    }
    return result;
}

bool MemoryMgr::is_in_live_set( Object *object )
{
    ObjectSet_t::iterator iter = this->m_live_set.find( object );
    return (iter != this->m_live_set.end());
}
