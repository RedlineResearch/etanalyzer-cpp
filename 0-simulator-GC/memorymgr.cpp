#include "memorymgr.h"

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
    return false;
}

//---------------------------------------------------------------------------------
//===========[ MemoryMgr ]=========================================================

// Initialize all the memory and regions
// Takes a std::vector list of sizes.
// Assuming index of size corresponds to level
bool MemoryMgr::initialize_memory( vector<int> sizes )
{
    assert(sizes.size() > 0);
    for ( vector<int>::iterator iter = sizes.begin();
          iter != sizes.end();
          ++iter ) {
        new_region( MemoryMgr::ALLOC,
                    *iter,
                    0 ); // Level 0 is required.
    }
    return true;
}

// Returns true if allocation caused garbage collection.
//         false otherwise.
bool MemoryMgr::allocate( Object *object,
                          unsigned int create_time )
{
    return false;
}

// Create new region with the given name.
// Returns a reference to the region.
Region & MemoryMgr::new_region( string &region_name,
                                unsigned int region_size,
                                int level )
{
    RegionMap::iterator iter = this->m_region_map.find(region_name);
    // Blow up if we create a new region with the same name.
    assert(iter == this->m_region_map.end());
    assert(level >= 0); // TODO make this more informative
    Region *regptr = new Region( region_name, region_size, level );
    assert(regptr); // TODO make this more informative
    this->m_region_map[region_name] = regptr;
    return *regptr;
}
