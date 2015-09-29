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

void HeapState::end_of_program(unsigned int cur_time)
{
    // -- Set death time of all remaining live objects
    for ( ObjectMap::iterator i = m_objects.begin();
          i != m_objects.end();
          ++i ) {
        Object* obj = (*i).second;
        if (obj->isLive(cur_time)) {
            obj->makeDead(cur_time);
        }
    }
}

void HeapState::set_candidate(unsigned int objId)
{
    m_candidate_map[objId] = true;
}

void HeapState::unset_candidate(unsigned int objId)
{
    m_candidate_map[objId] = false;
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

    EdgeMap::iterator p = m_fields.find(field_id);
    if (p != m_fields.end()) {
        // -- Old edge
        Edge* old_edge = p->second;
        if (old_edge) {
            // -- Now we know the end time
            old_edge->setEndTime(cur_time);

        }
    }

    // -- Increment new ref
    target->incrementRefCount();

    // -- Do store
    m_fields[field_id] = edge;

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
            for ( EdgeMap::iterator p = m_fields.begin();
                  p != m_fields.end();
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

void Object::makeDead(unsigned int death_time)
{
    // -- Record the death time
    m_deathTime = death_time;

    // -- Visit all edges
    for ( EdgeMap::iterator p = m_fields.begin();
          p != m_fields.end();
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
    for ( EdgeMap::iterator p = m_fields.begin();
          p != m_fields.end();
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
    m_color = newColor;
}

