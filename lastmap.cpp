#include "lastmap.h"


// TODO Doc
LastEvent LastMap::getLastEvent( threadId_t tid )
{
    _LastMap_t::iterator it = m_map.find(tid);
    pair<LastEvent, Object *> result = this->getLastEventAndObject( tid );
    return result.first;
}

// TODO Doc
Object * LastMap::getLastObject( threadId_t tid )
{
    _LastMap_t::iterator it = m_map.find(tid);
    pair<LastEvent, Object *> result = this->getLastEventAndObject( tid );
    return result.second;
}

// TODO Doc
pair<LastEvent, Object *> LastMap::getLastEventAndObject( threadId_t tid )
{
    _LastMap_t::iterator it = m_map.find(tid);
    if (it != m_map.end()) {
        return it->second;
    } else {
        if (this->m_update_flag) {
            return std::make_pair( LastEvent::UPDATE, this->m_last_update.second );
        } else {
            return std::make_pair( LastEvent::UNKNOWN_EVENT, (Object *) NULL );
        }
    }
}

// TODO Doc
void LastMap::setLast( threadId_t tid, LastEvent event, Object * obj )
{
    if (event == LastEvent::ROOT) {
        this->m_map[tid] = std::make_pair( event, obj );
    } else {
        this->m_last_update = std::make_pair( tid, obj );
        this->m_update_flag = true;
    }
}
