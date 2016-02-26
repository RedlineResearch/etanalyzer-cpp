#ifndef SUMMARY_H
#define SUMMARY_H

// ----------------------------------------------------------------------
//  Summary of key object statistics
//
//    - Using struct to simplify things. If need be this can be converted
//      into a class with all the OO insanity. Note that structs are classes
//      in C++ with everything public by default.

class Summary
{
public:
    unsigned int num_objects;
    unsigned int size;
    unsigned int num_groups;

    Summary( unsigned int num_obj, unsigned int s, unsigned num_g )
        : num_objects(num_obj)
        , size(s)
        , num_groups(num_g) {}
};

#endif
