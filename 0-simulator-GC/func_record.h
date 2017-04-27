#ifndef FUNC_RECORD_H
#define FUNC_RECORD_H

// ----------------------------------------------------------------------
//   Representation of objects on the heap
//
#include <iostream>
#include <map>
#include <limits.h>
#include <assert.h>
// TODO #include <algorithm>
// TODO #include <deque>
// TODO using namespace boost;
// TODO using namespace boost::logic;

typedef std::set< unsigned int > TypeSet_t;

class Func_Record
{
    public:
        Func_Record()
            : total_objects(0)
            , total_bytes(0)
            , total_called(0)
        {
        }

        unsigned int getTotalObjects() const
        {
            return this->total_objects;
        }
        unsigned int getTotalBytes() const
        {
            return this->total_bytes;
        }
        unsigned int getTotalCalled() const
        {
            return this->total_called;
        }
        unsigned int getMaxBytesPerCall() const
        {
            return this->max_bytes_per_call;
        }
        unsigned int getMinBytesPerCall() const
        {
            return this->min_bytes_per_call;
        }

        unsigned int setTotalObjects( unsigned int my_total_objects )
        {
            return (this->total_objects = my_total_objects);
        }
        unsigned int setTotalBytes( unsigned int my_total_bytes )
        {
            return (this->total_bytes = my_total_bytes);
        }
        unsigned int setTotalCalled ( unsigned int my_total_called )
        {
            return (this->total_called = my_total_called);
        }
        unsigned int setMaxBytesPerCall( unsigned int my_max_bytes_per_call )
        {
            return (this->max_bytes_per_call = my_max_bytes_per_call);
        }
        unsigned int setMinBytesPerCall( unsigned int my_min_bytes_per_call )
        {
            return (this->min_bytes_per_call = my_min_bytes_per_call);
        }

        void end_function_update( TypeSet_t &types_dead,
                                  unsigned int num_objects_dead,
                                  unsigned int bytes_dead )
        {
            this->total_objects += num_objects_dead;
            this->total_bytes += bytes_dead;
            this->classSet.insert( types_dead.begin(), types_dead.end() );
        }

    private:
        unsigned int total_objects;
        unsigned int total_bytes;
        unsigned int total_called;
        unsigned int max_bytes_per_call;
        unsigned int min_bytes_per_call;
        TypeSet_t classSet; // containting class Ids 
};


#endif // FUNC_RECORD_H
