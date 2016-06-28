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
    // TODO If object is already in the set, log a warning message.
    this->m_live_set.insert( object );
    this->m_free -= objSize; // Free goes down.
    this->m_used += objSize; // Used goes up.

    return true; // TODO Do we want to do something else different
    // if object is already in the set?
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

bool Region::add_to_garbage_set( Object *object )
{
    ObjectSet_t::iterator iter = this->m_live_set.find(object);
    object->setGarbageFlag();
    if (iter == this->m_live_set.end()) {
        // Not in live set.
        cerr << "X";
        return false;
    }
    unsigned int objSize = object->getSize();
    // Remove from live_set
    this->m_live_set.erase(iter);
    this->m_live -= objSize; // Live goes down.
    // Add to garbage waiting set
    this->m_garbage_waiting.insert(object);
    this->addToGarbage( objSize );
    return true;
}

bool Region::makeDead( Object *object )
{
    // Found object.
    // Remove from m_live_set and put into m_garbage_waiting
    bool flag = false;
    if (!object->isGarbage()) {
        flag = this->add_to_garbage_set( object );
        // TODO: Anything else I need to do here?
    }
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
    // Decisions for collection should be done here at the MemoryMgr level.
    bool done = this->m_alloc_region->allocate( object, create_time );
    if (!done) {
        // Not enough free space.
        int collected = this->m_alloc_region->collect( create_time );
        // Try again.
        done = this->m_alloc_region->allocate( object, create_time );
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
