#include "memorymgr.h"

// -- Global flags
bool MemoryMgr::debug = false;
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
                                unsigned int region_size )
{
    RegionMap::iterator iter = this->m_region_map.find(region_name);
    // Blow up if we create a new region with the same name.
    assert(iter == this->m_region_map.end());
    Region *regptr = new Region( region_name, region_size );
    this->m_region_map[region_name] = regptr;
    return *regptr;
}
