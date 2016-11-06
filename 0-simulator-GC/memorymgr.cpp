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
    this->m_live += objSize; // Live also goes up.

    return true;
    // TODO: do i need create_time? And if yes, how do I save it?
}

// TODO bool Region::remove( Object *object )
// TODO {
// TODO     ObjectSet_t::iterator iter = this->m_live_set.find(object);
// TODO     if (iter == this->m_live_set.end()) {
// TODO         return false;
// TODO     }
// TODO     unsigned int objSize = object->getSize();
// TODO     this->m_live_set.erase(iter);
// TODO     this->m_free += objSize; // Free goes up.
// TODO     this->m_used -= objSize; // Used goes down.
// TODO     this->m_live -= objSize; // Live goes down.
// TODO     assert(this->m_free <= this->m_size);
// TODO     assert(this->m_used >= 0);
// TODO     return true;
// TODO }

void Region::add_to_garbage_set( Object *object )
{
    unsigned int objSize = object->getSize();
    // Livesize goes down.
    this->m_live -= objSize;
    // Add to garbage waiting set
    this->m_garbage_waiting.insert(object);
    // Remove from live set
    this->m_live_set.erase(object);
    // Keep a running total of how much garbage there is.
    this->m_garbage += objSize;
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
        cout << "ERROR: makeDead on object Id[ " << object->getId()
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
        cout << "ERROR: makeDead on object Id[ " << object->getId()
             << " ] already set to garbage.";
    }
    // Remove from live_set regardless.
    this->m_live_set.erase(object);
    // Note that we don't adjust the sizes here. This is done in
    // 'add_to_garbage_set'.
    // return whether or not we were able to make the object garbage.
    return flag;
}

int Region::collect( unsigned int timestamp )
{
    // Clear the garbage waiting set and return the space to free.
    int collected = this->m_garbage;
    this->GC_attempts++;

    this->m_garbage_waiting.clear();
    // Garbage in this region is now 0.
    this->setGarbage(0);
    // TODO TODO: This is only DEBUG TODO TODO
    cout << "GC[ " << timestamp << ", " << collected << "]" << endl;
    // TODO TODO: End DEBUG
    // Add the collected space back to free
    this->m_free += collected;
    assert(this->m_free <= this->m_size); // Sanity check
    // Record this collection
    GCRecord_t rec = make_pair( timestamp, collected );
    this->m_gc_history.push_back( rec );
    // Return how much we collected
    return collected;
}

int Region::setGarbage( int newval )
{
    this->m_garbage = newval;
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
    // ++iter;
    // ++level;
    // string myname("OTHER");
    // while (iter != sizes.end()) {
    //     this->new_region( myname,
    //                       *iter,
    //                       level );
    //     ++iter;
    //     ++level;
    // }
    if (this->m_alloc_region) {
        this->m_total_size += *iter;
    } else {
        cerr << "Unable to allocate in our simulator. REAL OOM in your system. Quitting." << endl;
        exit(1);
    }
    // Calculate our GC threshold for the system.
    this->m_GC_byte_threshold = static_cast<int>(this->m_total_size * this->m_GC_threshold);
    if ( (this->m_GC_byte_threshold == 0) ||
         (this->m_GC_byte_threshold > this->m_total_size) ){
        cerr << "Invalid GC byte threshold: " << this->m_GC_byte_threshold << endl;
        cerr << "GC percentage threshold  : " << this->m_GC_threshold << endl;
        cerr << "Total heap size          : " << this->m_total_size << endl;
        exit(2);
    }

    return true;
}

// Do a garbage collection
// Returns number of bytes collected
int MemoryMgr::do_collection()
{
    return 0;
}

// Returns true if GC threshold has been exceeded
//         false otherwise.
bool MemoryMgr::should_do_collection()
{
    // Assume that threshold is always valid.
    return (this->m_liveSize >= this->m_GC_byte_threshold);
}

// Returns true if allocation caused garbage collection.
//         false otherwise.
bool MemoryMgr::allocate( Object *object,
                          unsigned int create_time,
                          unsigned int new_alloc_time )
{
    assert(this->m_alloc_region);
    int collected = 0; // Amount collected
    bool GCdone = false;

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
        // 1. We collect on a failed allocation.
        collected = this->m_alloc_region->collect( create_time );
        GCdone = true;
        // 2. Try again. Note that we only try one more time as the 
        //    basic collector will give back all possible free memory.
        done = this->m_alloc_region->allocate( object, create_time );
        // NOTE: In a setup with more than one region, the MemoryMgr could
        // go through all regions trying to find free space. And returning
        // 'false' means an Out Of Memory Error (OOM).
    }
    if (done) {
        unsigned long int temp = this->m_liveSize + object->getSize();
        // Max live size calculation
        // We silently peg to ULONG_MAX the wraparound.
        // TODO: Maybe we should just error here as this probably isn't possible.
        this->m_liveSize = ( (temp < this->m_liveSize) ? ULONG_MAX : temp );
        // Add to live set.
        this->m_live_set.insert( object );
        // Keep tally of what our maximum live size is for the program run
        if (this->m_maxLiveSize < this->m_liveSize) {
            this->m_maxLiveSize = this->m_liveSize;
        }
        if (!GCdone && this->should_do_collection()) {
            collected += this->m_alloc_region->collect( create_time );
            GCdone = true;
        }
    }
    if (GCdone) {
        // Increment the GC count
        this->m_times_GC++;
    }
    return done;
}

// Create new region with the given name.
// Returns a reference to the region.
Region *MemoryMgr::new_region( string &region_name,
                               unsigned int region_size,
                               int level )
{
    // Debug
    cerr << "Creating a new region[ " << region_name << " ]: "
         << " size = " << region_size
         << "  | level = " << level << endl;
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
    unsigned long int temp = this->m_liveSize - object->getSize();
    if (temp > this->m_liveSize) {
        // OVERFLOW, underflow?
        this->m_liveSize = 0;
        cout << "UNDERFLOW of substraction." << endl;
        // TODO If this happens, maybe we should think about why it happens.
    } else {
        // All good. Fight on.
        this->m_liveSize = temp;
    }
    this->m_alloc_region->makeDead( object );
    if (!object->isDead()) {
        object->makeDead( death_time, this->m_alloc_time );
    }
    // TODO: Think about whether calling object->makeDead is better in region::makeDead
    return true;
}

// On an U(pdate) event
void MemoryMgr::addEdge( EdgeId_t source,
                         EdgeId_t target )
{

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

//==============================================================================
// Debug functions
void Region::print_status()
{
    cout << "Region[ " << this->m_name << " ]" << endl
         << "    - size: " << this->m_size << endl
         << "    - live: " << this->m_live << endl
         << "    - free: " << this->m_free << endl
         << "    - used: " << this->m_used << endl;
}

void MemoryMgr::print_status()
{
}

unsigned long int MemoryMgr::get_num_GC_attempts( bool printflag )
{
    // For every region that we manage:

    //      get the number of GC attempts per region.
    //      if printflag: print
}
