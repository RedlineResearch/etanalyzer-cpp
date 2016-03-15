#include "memorymgr.h"
#include "heap.h"

// -- Global flags
bool MemoryMgr::debug = false;
string MemoryMgr::ALLOC = "ALLOC";
bool Region::debug = false;

// TODO using namespace boost;

//---------------------------------------------------------------------------------
//===========[ Region ]============================================================

// Returns true if allocation was successful.
//         false otherwise.
bool Region:: allocate( Object *object,
                        unsigned int create_time )
{
    // Check to see if there's space
    unsigned int objSize = object->getSize();
    if (objSize > this->m_free) {
        // Not enough free space.
        int collected = this->collect( create_time );
        // Try again.
        if (objSize > this->m_free) {
            return false;
        }
    }
    // TODO If object is already in the set, log a warning message.
    this->m_live_set.insert( object );
    this->m_free -= objSize; // Free goes down.
    this->m_used += objSize; // Used goes up.
    assert(this->m_used <= this->m_size); // Invariant check.
    return true; // TODO Do we want to do something else different?
    // Maybe false if object is already in the set?
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

bool Region::add_to_garbage( Object *object )
{
    ObjectSet_t::iterator iter = this->m_live_set.find(object);
    if (iter == this->m_live_set.end()) {
        // Not in live set.
        return false;
    }
    unsigned int objSize = object->getSize();
    // Remove from live_set
    this->m_live_set.erase(iter);
    // Add to garbage waiting set
    this->m_garbage_waiting.insert(object);
    // Adjust the status variables.
    this->m_live -= objSize; // Live goes down.
    this->m_garbage += objSize; // Garbage goes up.
    assert(this->m_live >= 0);
    assert(this->m_garbage <= this->m_size);
    return true;
}

bool Region::makeDead( Object *object )
{
    // Found object.
    // Remove from m_live_set and put into m_garbage_waiting
    bool flag = this->add_to_garbage( object );
}

int Region::collect( unsigned int timestamp )
{
    // Clear the garbage waiting set and return the space to free.
    int collected = this->m_garbage;

    this->m_garbage_waiting.clear();
    this->m_garbage = 0;
    this->m_free += collected;
    this->m_gc_history.push_back( collected );
    return collected;
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
    this->m_alloc_region = new_region( MemoryMgr::ALLOC,
                                       *iter,
                                       level ); // Level 0 is required.
    ++iter;
    ++level;
    string myname("OTHER");
    while (iter != sizes.end()) {
        new_region( myname,
                    *iter,
                    level );
        ++iter;
        ++level;
    }
    return true;
}

// Returns true if allocation caused garbage collection.
//         false otherwise.
bool MemoryMgr::allocate( Object *object,
                          unsigned int create_time )
{
    assert(this->m_alloc_region);
    return this->m_alloc_region->allocate( object, create_time );
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
