#ifndef LASTMAP_H
#define LASTMAP_H

#include <map>
#include <utility>

#include "heap.h"

typedef map<unsigned int, pair<LastEvent, Object *>> _LastMap_t;
typedef unsigned int threadId_t;

// ----------------------------------------------------------------------
//   Maps thread IDs to events and object pointers
//

class LastMap {
    public:
        // Constructor
        LastMap()
            : m_map()
            , m_last_update(std::make_pair(0, (Object *) NULL))
            , m_update_flag(false) {
        }

        LastEvent getLastEvent( threadId_t tid );
        Object * getLastObject( threadId_t tid );
        pair<LastEvent, Object *> getLastEventAndObject( threadId_t tid );
        void setLast( threadId_t tid, LastEvent event, Object * obj );
    private:
        _LastMap_t m_map;
        pair<threadId_t, Object *> m_last_update;
        bool m_update_flag;
};
#endif // LASTMAP_H
