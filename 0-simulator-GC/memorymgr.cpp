#include "memorymgr.h"

// -- Global flags
bool MemoryMgr::debug = false;
bool Region::debug = false;

// TODO using namespace boost;

// Returns true if allocation was successful.
//         false otherwise.
bool Region:: allocate( Object *object,
                        unsigned int create_time )
{
    return false;
}

bool MemoryMgr::allocate( Object *object,
                          unsigned int create_time )
{
    return false;
}
