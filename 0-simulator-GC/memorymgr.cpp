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
        // cout << "ERROR: makeDead on object Id[ " << object->getId()
        //      << " ] but can not find in live set.";
        return false;
    }
    if (!object->isGarbage()) {
        this->add_to_garbage_set( object );
        flag = true;
        // If flag (result) is false, that means I tried to add to 
        // the garbage set but didn't find it there.
    } else {
        // What do we do if the object was already garbage?
        // cout << "ERROR: makeDead on object Id[ " << object->getId()
        //      << " ] already set to garbage.";
    }
    // Remove from live_set regardless.
    this->m_live_set.erase(object);
    // Note that we don't adjust the sizes here. This is done in
    // 'add_to_garbage_set'.
    // return whether or not we were able to make the object garbage.
    return flag;
}

int Region::collect( unsigned int timestamp,
                     unsigned int timestamp_alloc )
{
    // Clear the garbage waiting set and return the space to free.
    int collected = this->m_garbage;
    this->GC_attempts++;

    this->m_garbage_waiting.clear();
    // Garbage in this region is now 0.
    this->setGarbage(0);
    // TODO TODO: This is only DEBUG TODO TODO
    cout << "GC[" << timestamp << "," << timestamp_alloc << ","
         << collected << "]" << endl;
    // TODO TODO: End DEBUG
    // Add the collected space back to free
    this->m_free += collected;
    assert(this->m_free <= this->m_size); // Sanity check
    // Record this collection
    GCRecord_t rec = make_pair( timestamp_alloc, collected );
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
    // OLD CODE: when GC threshold is less than total size
    //           this->m_GC_byte_threshold = static_cast<int>(this->m_total_size * this->m_GC_threshold);
    this->m_GC_byte_threshold = this->m_total_size;
    return true;

    // NOT NEEDED NOW: if ( (this->m_GC_byte_threshold == 0) ||
    // NOT NEEDED NOW:      (this->m_GC_byte_threshold > this->m_total_size) ){
    // NOT NEEDED NOW:     cerr << "Invalid GC byte threshold: " << this->m_GC_byte_threshold << endl;
    // NOT NEEDED NOW:     cerr << "GC percentage threshold  : " << this->m_GC_threshold << endl;
    // NOT NEEDED NOW:     cerr << "Total heap size          : " << this->m_total_size << endl;
    // NOT NEEDED NOW:     exit(2);
    // NOT NEEDED NOW: }
}

// Initialize the grouped region of objects
void MemoryMgr::initialize_special_group( string &group_filename,
                                          int numgroups )
{
    std::ifstream infile( group_filename );
    string line;
    string s;
    this->m_numgroups = numgroups;
    int group_count = 0;
    // The file is a CSV file with the following header:
    //    groupId,number,death_time,list
    // First line is a header:
    std::getline(infile, line);
    // TODO: Maybe make sure we have the right file?
    while (std::getline(infile, line)) {
        size_t pos = 0;
        string token;
        unsigned long int num;
        int count = 0;
        //------------------------------------------------------------
        // Get the group Id
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "GID: " << s << endl;
        int groupId = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the number of objects in the group
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "NUM: " << s << endl;
        int total = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the deathtime
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "DTIME: " << s << endl;
        int dtime = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the object Ids
        while ((pos = line.find(",")) != string::npos) {
            token = line.substr(0, pos);
            num = std::stoi(token);
            line.erase(0, pos + 1);
            ++count;
            this->m_specgroup.insert(num);
        }
        // Get the last number
        num = std::stoi(line);
        this->m_specgroup.insert(num);
        ++count;
        ++group_count;
        // DEBUG
        cout << "Special group[ " << groupId << " ]:  "
             << "total objects read in = " << total << " | "
             << "set size = " << this->m_specgroup.size() << endl;
        // END DEBUG
        if (group_count >= numgroups) {
            break;
        }
    }
}


// TODO // Do a garbage collection
// TODO // Returns number of bytes collected
// TODO int MemoryMgr::do_collection()
// TODO {
// TODO     return 0;
// TODO }

// Returns true if GC threshold has been exceeded
//         false otherwise.
bool MemoryMgr::should_do_collection()
{
    assert(false);
    // NOT NEEDED NOW.
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
        collected = this->m_alloc_region->collect( create_time, new_alloc_time );
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
        // NOT NEEDED NOW: if (!GCdone && this->should_do_collection()) {
        // NOT NEEDED NOW:     collected += this->m_alloc_region->collect( create_time );
        // NOT NEEDED NOW:     GCdone = true;
        // NOT NEEDED NOW: }
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
    bool result = this->m_alloc_region->makeDead( object );
    if (!object->isDead()) {
        object->makeDead( death_time, this->m_alloc_time );
    }
    // TODO: Think about whether calling object->makeDead is better in region::makeDead
    return result;
}

// On an U(pdate) event
void MemoryMgr::add_edge( ObjectId_t src,
                          ObjectId_t tgt )
{
    // DEBUG
    // DEBUG cout << "Adding edge (" << src << "," << tgt << ")" << endl;
    ObjectIdSet_t::iterator iter = this->m_specgroup.find(src);
    ObjectIdSet_t::iterator tgt_iter = this->m_specgroup.find(tgt);
    //----------------------------------------------------------------------
    // Add to edge maps
    if (iter != this->m_specgroup.end()) {
        // Source is in special group
        if (tgt_iter != this->m_specgroup.end()) {
            // Target is in special group
            ObjectId2SetMap_t::iterator itmp = this->m_region_edges.find(src);
            if (itmp != this->m_region_edges.end()) {
                // Already in the map
                ObjectIdSet_t &myset = itmp->second;
                myset.insert(tgt);
            } else {
                // Not in the map
                ObjectIdSet_t myset;
                myset.insert(tgt);
                this->m_region_edges[src] = myset;
            }
            // // DEBUG ONLY
            // itmp = this->m_region_edges.find(src);
            // assert(itmp != this->m_region_edges.end());
            // ObjectIdSet_t &myset = itmp->second;
            // ObjectIdSet_t::iterator tmpiter = myset.find(tgt);
            // assert(tmpiter != myset.end());
            // // END DEBUG
            this->m_region_edges_count++;
        } else {
            // Target is NOT in special group
            ObjectId2SetMap_t::iterator itmp = this->m_out_edges.find(src);
            if (itmp != this->m_out_edges.end()) {
                // Already in the map
                ObjectIdSet_t &myset = itmp->second;
                myset.insert(tgt);
            } else {
                // Not in the map
                ObjectIdSet_t myset;
                myset.insert(tgt);
                this->m_out_edges[src] = myset;
            }
            this->m_out_edges_count++;
        }
    } else {
        // Source is NOT in special group
        if (tgt_iter != this->m_specgroup.end()) {
            // Target is in special group
            ObjectId2SetMap_t::iterator itmp = this->m_in_edges.find(src);
            if (itmp != this->m_in_edges.end()) {
                // Already in the map
                ObjectIdSet_t &myset = itmp->second;
                myset.insert(tgt);
            } else {
                // Not in the map
                ObjectIdSet_t myset;
                myset.insert(tgt);
                this->m_in_edges[src] = myset;
            }
            this->m_in_edges_count++;
        } else {
            // Target is NOT in special group
            ObjectId2SetMap_t::iterator itmp = this->m_nonregion_edges.find(src);
            if (itmp != this->m_nonregion_edges.end()) {
                // Already in the map
                ObjectIdSet_t &myset = itmp->second;
                myset.insert(tgt);
            } else {
                // Not in the map
                ObjectIdSet_t myset;
                myset.insert(tgt);
                this->m_nonregion_edges[src] = myset;
            }
            this->m_nonregion_edges_count++;
        }
    }
    //----------------------------------------------------------------------
    // Add to look up maps
    // Src map
    ObjectId2SetMap_t::iterator miter = this->m_srcidmap.find(src);
    if (miter != this->m_srcidmap.end()) {
        // Already exists
        this->m_srcidmap[src].insert(tgt);
    } else {
        // Doesn't exist
        ObjectIdSet_t tmpset;
        tmpset.insert(tgt);
        this->m_srcidmap[src] = tmpset;
    }
    //----------------------------------------------------------------------
    // Tgt map
    miter = this->m_tgtidmap.find(tgt);
    if (miter != this->m_tgtidmap.end()) {
        // Already exists
        this->m_tgtidmap[src].insert(src);
    } else {
        // Doesn't exist
        ObjectIdSet_t tmpset;
        tmpset.insert(src);
        this->m_tgtidmap[tgt] = tmpset;
    }
} 
void MemoryMgr::remove_edge( ObjectId_t src,
                             ObjectId_t oldTgtId )
{
    // DEBUG
    // DEBUG cout << "Remove edge (" << src << "," << oldTgtId << ")" << endl;
    ObjectId2SetMap_t::iterator iter;
    this->m_attempts_edges_removed++;
    //----------------------------------------------------------------------
    // Remove edge from region maps
    // Look in the special region
    iter = this->m_region_edges.find(src);
    if (iter != this->m_region_edges.end()) {
        // Found in region
        ObjectIdSet_t &myset = iter->second;
        myset.erase(oldTgtId);
        this->m_edges_removed++;
        goto END;
    }
    // Look outside the special region
    iter = this->m_nonregion_edges.find(src);
    if (iter != this->m_nonregion_edges.end()) {
        // Found in nonregion
        ObjectIdSet_t &myset = iter->second;
        myset.erase(oldTgtId);
        this->m_edges_removed++;
        goto END;
    }
    // Look in the in to out 
    iter = this->m_in_edges.find(src);
    if (iter != this->m_in_edges.end()) {
        // Found in IN region 
        ObjectIdSet_t &myset = iter->second;
        myset.erase(oldTgtId);
        this->m_edges_removed++;
        goto END;
    }
    // Look in the out to in 
    iter = this->m_out_edges.find(src);
    if (iter != this->m_out_edges.end()) {
        // Found in IN region 
        ObjectIdSet_t &myset = iter->second;
        myset.erase(oldTgtId);
        this->m_edges_removed++;
        goto END;
        // Well this isn't needed but symmetric.
        // If ever there's any new code after this, this makes it less likely
        // that a bug's introduced.
    }
    END:
    return;
}

// - TODO
void MemoryMgr::remove_from_srcidmap( ObjectId_t src,
                                      ObjectId_t oldTgtId )
{
    //----------------------------------------------------------------------
    // Remove from appropriate lookup maps
    ObjectId2SetMap_t::iterator miter = this->m_srcidmap.find(src);
    if (miter != this->m_srcidmap.end()) {
        // Found the source.
        // Look for the old target id in the set
        ObjectIdSet_t &myset = miter->second;
        ObjectIdSet_t::iterator itmp = myset.find(oldTgtId);
        if (itmp != myset.end()) {
            // Remove old target
            myset.erase(itmp);
        }
        // DEBUG
        // itmp = myset.find(oldTgtId);
        // assert( itmp == myset.end() );
    }
}

// - TODO
void MemoryMgr::remove_from_tgtidmap( ObjectId_t src,
                                      ObjectId_t tgtId )
{
    ObjectId2SetMap_t::iterator miter = this->m_tgtidmap.find(tgtId);
    if (miter != this->m_tgtidmap.end()) {
        // Found the old target.
        // Look for the source id in the set
        ObjectIdSet_t &myset = miter->second;
        ObjectIdSet_t::iterator itmp = myset.find(src);
        if (itmp != myset.end()) {
            myset.erase(itmp);
        }
        // DEBUG
        // itmp = myset.find(src);
        // assert( itmp == myset.end() );
    }
}

// - TODO Documentation
void MemoryMgr::remove_object( ObjectId_t objId )
{
    ObjectId2SetMap_t::iterator iter;
    //------------------------------------------------------------
    // Look for objId in m_srcidmap
    iter = this->m_srcidmap.find(objId);
    if (iter != this->m_srcidmap.end()) {
        // Found the objId as source.
        // Go through the and remove all outgoing edges:
        ObjectIdSet_t &myset = iter->second;
        ObjectIdSet_t::iterator siter = myset.begin();
        while (siter != myset.end()) {
            // Remove target (src, *siter)
            this->remove_edge( objId, *siter );
            siter++;
        }
        this->m_srcidmap.erase(iter);
    }
    //------------------------------------------------------------
    // Look for objId in m_tgtidmap
    iter = this->m_tgtidmap.find(objId);
    if (iter != this->m_tgtidmap.end()) {
        // Found the objId as target.
        // Go through the and remove all incoming edges:
        ObjectIdSet_t &myset = iter->second;
        ObjectIdSet_t::iterator siter = myset.begin();
        while (siter != myset.end()) {
            // Remove target (src, *siter)
            this->remove_edge( *siter, objId );
            siter++;
        }
        this->m_tgtidmap.erase(iter);
    }
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
