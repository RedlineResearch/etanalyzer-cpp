#include "heap.h"

// -- Global flags
bool HeapState::do_refcounting = true;
bool HeapState::debug = false;

Object* HeapState::allocate( unsigned int id, unsigned int size,
                             char kind, char* type, AllocSite* site, 
                             unsigned int els, Thread* thread,
                             unsigned int create_time )
{
    Object* obj = new Object( id, size,
                              kind, type,
                              site, els,
                              thread, create_time,
                              this );
    m_objects[obj->getId()] = obj;

    if (m_objects.size() % 100000 == 0) {
        cout << "OBJECTS: " << m_objects.size() << endl;
    }

    return obj;
}

// -- Manage heap
Object* HeapState::get(unsigned int id)
{
    ObjectMap::iterator p = m_objects.find(id);
    if (p != m_objects.end()) {
        return (*p).second;
    }
    else {
        return 0;
    }
}

Edge* HeapState::make_edge( Object* source, unsigned int field_id,
                            Object* target, unsigned int cur_time )
{
    Edge* new_edge = new Edge( source, field_id,
                               target, cur_time );
    m_edges.insert(new_edge);

    if (m_edges.size() % 100000 == 0) {
        cout << "EDGES: " << m_edges.size() << endl;
    }

    return new_edge;
}

// TODO Documentation :)
void HeapState::end_of_program(unsigned int cur_time)
{
    // -- Set death time of all remaining live objects
    for ( ObjectMap::iterator i = m_objects.begin();
          i != m_objects.end();
          ++i ) {
        Object* obj = i->second;
        if (obj->isLive(cur_time)) {
            obj->makeDead(cur_time);
        }
    }
}

// TODO Documentation :)
void HeapState::set_candidate(unsigned int objId)
{
    m_candidate_map[objId] = true;
}

// TODO Documentation :)
void HeapState::unset_candidate(unsigned int objId)
{
    m_candidate_map[objId] = false;
}

// TODO Documentation :)
void HeapState::process_queue()
{
}

// TODO Documentation :)
void HeapState::analyze()
{
}

// TODO Documentation :)
deque< deque<int> > HeapState::scan_queue()
{
    deque< deque<int> > result;
    for ( map<unsigned int, bool>::iterator i = m_candidate_map.begin();
          i != m_candidate_map.end();
          ++i ) {
        int objId = i->first;
        bool flag = i->second;
        if (flag) {
            Object* object = this->get(objId);
            if (object) {
                if (object->getColor() == BLACK) {
                    object->mark_red();
                    object->scan();
                    deque<int> cycle = object->collect_blue();
                    if (cycle.size() > 0) {
                        result.push_back( cycle );
                    }
                }
            }
        }
    }
    return result;
}

// -- Return a string with some information
string Object::info() {
    stringstream ss;
    ss << "OBJ 0x"
       << hex
       << m_id
       << dec
       << "("
       << m_type << " "
       << (m_site != 0 ? m_site->info() : "<NONE>")
       << " @"
       << m_createTime
       << ")";
    return ss.str();
}

void Object::updateField(Edge* edge, unsigned int cur_time)
{
    unsigned int field_id = edge->getSourceField();
    Object* target = edge->getTarget();

    EdgeMap::iterator p = this->m_fields.find(field_id);
    if (p != this->m_fields.end()) {
        // -- Old edge
        Edge* old_edge = p->second;
        if (old_edge) {
            // -- Now we know the end time
            old_edge->setEndTime(cur_time);
            deleteEdge(old_edge);
        }
    }

    // -- Increment new ref
    if (target) {
        target->incrementRefCount();
        // TODO: An increment of the refcount means this isn't a candidate root
        //       for a garbage cycle.
    }

    // -- Do store
    this->m_fields[field_id] = edge;

    if (HeapState::debug) {
        cout << "Update "
             << m_id << "." << field_id
             << " --> " << target->m_id
             << " (" << target->getRefCount() << ")"
             << endl;
    }
}

void Object::deleteEdge(Edge* edge)
{
    Object* target = edge->getTarget();
    if (target) {
        target->decrementRefCount();
        int rc = target->getRefCount();
        if (rc == 0) {
            // -- Visit all edges
            for ( EdgeMap::iterator p = target->m_fields.begin();
                  p != target->m_fields.end();
                  p++ ) {
                Edge* target_edge = p->second;
                Object* next_target_object = target_edge->getTarget();
                if (target_edge) {
                    deleteEdge( target_edge );
                }
            }
        } else {
            Color color = target->getColor();
            if (color != BLACK) {
                unsigned int objId = target->getId();
                target->recolor(BLACK);
                m_heapptr->set_candidate(objId);
            }
        }
    }
}

void Object::mark_red()
{
    if ( (m_color == GREEN) || (m_color == BLACK) ) {
        // Only recolor if object is GREEN or BLACK.
        // Ignore if already RED or BLUE.
        this->recolor( RED );
        for ( EdgeMap::iterator p = this->m_fields.begin();
              p != this->m_fields.end();
              p++ ) {
            Edge* edge = p->second;
            Object* target = edge->getTarget();
            target->mark_red();
        }
    }
}

void Object::scan()
{
    if (this->m_color == RED) {
        if (this->m_refCount > 0) {
            this->scan_green();
        } else {
            this->recolor( BLUE );
            // -- Visit all edges
            for ( EdgeMap::iterator p = this->m_fields.begin();
                  p != this->m_fields.end();
                  p++ ) {
                Edge* target_edge = p->second;
                if (target_edge) {
                    Object* next_target_object = target_edge->getTarget();
                    if (next_target_object) {
                        next_target_object->scan();
                    }
                }
            }
        }
    }
}

void Object::scan_green()
{
    this->recolor( GREEN );
    for ( EdgeMap::iterator p = this->m_fields.begin();
          p != this->m_fields.end();
          p++ ) {
        Edge* target_edge = p->second;
        if (target_edge) {
            Object* next_target_object = target_edge->getTarget();
            if (next_target_object) {
                if (next_target_object->getColor() != GREEN) {
                    next_target_object->scan_green();
                }
            }
        }
    }
}

deque<int> Object::collect_blue()
{
    deque<int> result;
    if (this->getColor() == BLUE) {
        this->recolor( GREEN );
        for ( EdgeMap::iterator p = this->m_fields.begin();
              p != this->m_fields.end();
              p++ ) {
            Edge* target_edge = p->second;
            if (target_edge) {
                Object* next_target_object = target_edge->getTarget();
                if (next_target_object) {
                    result = next_target_object->collect_blue();
                }
            }
        }
        result.push_back( this->getId() );
    }
    return result;
}

void Object::makeDead(unsigned int death_time)
{
    // -- Record the death time
    m_deathTime = death_time;

    // -- Visit all edges
    for ( EdgeMap::iterator p = this->m_fields.begin();
          p != this->m_fields.end();
          p++ ) {
        Edge* edge = p->second;

        // -- Edge dies now
        edge->setEndTime(death_time);

        // TODO: Is this the right thing to do?
        // // -- Decrement outgoing refs
        // Object* target = edge->getTarget();
        // if (target) {
        //     target->decrementRefCount();
        // }
    }

    if (HeapState::debug) {
        cout << "Dead object " << m_id << " of type " << m_type << endl;
    }
}

void Object::recolor(Color newColor)
{
    // Maintain the invariant that the reference count of a node is
    // the number of GREEN or BLACK pointers to it.
    for ( EdgeMap::iterator p = this->m_fields.begin();
          p != this->m_fields.end();
          p++ ) {
        Edge* edge = p->second;
        Object* target = edge->getTarget();

        if ( (m_color == GREEN || m_color == BLACK) &&
             (newColor != GREEN) && (newColor != BLACK) ) {
            // decrement reference count of target
            if (target) {
                target->decrementRefCount();
            }
        } else if ( (m_color != GREEN && m_color != BLACK) &&
                    (newColor == GREEN) || (newColor == BLACK) ) {
            // increment reference count of target
            if (target) {
                target->incrementRefCount();
            }
        }
    }
    this->m_color = newColor;
}

