#include "heap.h"

// -- Global flags
bool HeapState::do_refcounting = true;
bool HeapState::debug = false;
unsigned int Object::g_counter = 0;

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
    assert(target != NULL);
    target->setPointedAtByHeap();

    if (m_edges.size() % 100000 == 0) {
        cout << "EDGES: " << m_edges.size() << endl;
    }

    return new_edge;
}

// TODO Documentation :)
void HeapState::end_of_program(unsigned int cur_time)
{
    unsigned int tmpcount = 0;
    // -- Set death time of all remaining live objects
    //    Also set the flags for the interesting classifications.
    for ( ObjectMap::iterator i = m_objects.begin();
          i != m_objects.end();
          ++i ) {
        Object* obj = i->second;
        if (obj->isLive(cur_time)) {
            obj->makeDead(cur_time);
        }
        // Do the count of heap vs stack loss here. TODO
        if (obj->getDiedByStackFlag()) {
            this->m_totalDiedByStack++;
            if (obj->wasPointedAtByHeap()) {
                this->m_diedByStackAfterHeap++;
            } else {
                this->m_diedByStackOnly++;
            }
        } else {
            // Setting the flag may not be necessary, but it may be useful
            // if code is written in the future that runs after
            // 'end_of_program' and depends on the flags being set.
            if (!obj->getDiedByHeapFlag()) {
                obj->setDiedByHeapFlag();
            }
            this->m_totalDiedByHeap++;
            // TODO: Is this interesting?
            //       if (obj->wasRoot()) { this->(); }
        }
        if (obj->wasLastUpdateNull()) {
            this->m_totalUpdateNull++;
        }
        // Save method death site to map
        Method *dsite = obj->getDeathSite();
        if (obj->getDiedByHeapFlag()) {
            // Died by heap
            if (dsite) {
                cout << "-";
                tmpcount++;
            } else {
                if (obj->wasDecrementedToZero()) {
                    cout << "\\";
                    // So died by heap but no saved death site. First alternative is
                    // to look for the a site that decremented to 0.
                    dsite = obj->getMethodDecToZero();
                } else {
                    cout << "?";
                    // TODO: No dsite here yet
                    // TODO TODO TODO
                }
                tmpcount++;
            }
        } else {
            // Died by stack
            assert(obj->getDiedByStackFlag());
            cout << "^";
            tmpcount++;
            // Died by stack flag. Look for last heap activity.
        }
        if (dsite) {
            DeathSitesMap::iterator it = this->m_death_sites_map.find(dsite);
            if (it == this->m_death_sites_map.end()) {
                this->m_death_sites_map[dsite] = new set<string>; 
            }
            this->m_death_sites_map[dsite]->insert(obj->getType());
        } else if (obj->getDiedByHeapFlag()) {
            // We couldn't find a deathsite for something that died by heap.
            // TODO ?????? TODO
            cout << "<" << endl;
        }
        if (tmpcount % 79 == 0) {
            cout << endl;
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
deque< deque<int> > HeapState::scan_queue( EdgeList& edgelist )
{
    deque< deque<int> > result;
    cout << "Queue size: " << this->m_candidate_map.size() << endl;
    for ( map<unsigned int, bool>::iterator i = this->m_candidate_map.begin();
          i != this->m_candidate_map.end();
          ++i ) {
        int objId = i->first;
        bool flag = i->second;
        if (flag) {
            Object* object = this->get(objId);
            if (object) {
                if (object->getColor() == BLACK) {
                    object->mark_red();
                    object->scan();
                    deque<int> cycle = object->collect_blue( edgelist );
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

void Object::updateField( Edge* edge,
                          unsigned int fieldId,
                          unsigned int cur_time,
                          Method *method )
{
    EdgeMap::iterator p = this->m_fields.find(fieldId);
    if (p != this->m_fields.end()) {
        // -- Old edge
        Edge* old_edge = p->second;
        if (old_edge) {
            // -- Now we know the end time
            Object* old_target = old_edge->getTarget();
            if (old_target) {
                old_target->decrementRefCountReal(cur_time, method);
            } 
            old_edge->setEndTime(cur_time);
        }
    }
    // -- Do store
    this->m_fields[fieldId] = edge;

    Object* target = NULL;
    if (edge) {
        target = edge->getTarget();
        // -- Increment new ref
        if (target) {
            target->incrementRefCountReal();
            // TODO: An increment of the refcount means this isn't a candidate root
            //       for a garbage cycle.
        }
    }

    if (HeapState::debug) {
        cout << "Update "
             << m_id << "." << fieldId
             << " --> " << (target ? target->m_id : 0)
             << " (" << (target ? target->getRefCount() : 0) << ")"
             << endl;
    }
}

void Object::mark_red()
{
    if ( (this->m_color == GREEN) || (this->m_color == BLACK) ) {
        // Only recolor if object is GREEN or BLACK.
        // Ignore if already RED or BLUE.
        this->recolor( RED );
        for ( EdgeMap::iterator p = this->m_fields.begin();
              p != this->m_fields.end();
              p++ ) {
            Edge* edge = p->second;
            if (edge) {
                Object* target = edge->getTarget();
                target->mark_red();
            }
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

deque<int> Object::collect_blue( EdgeList& edgelist )
{
    deque<int> result;
    if (this->getColor() == BLUE) {
        this->recolor( GREEN );
        result.push_back( this->getId() );
        for ( EdgeMap::iterator p = this->m_fields.begin();
              p != this->m_fields.end();
              p++ ) {
            Edge* target_edge = p->second;
            if (target_edge) {
                Object* next_target_object = target_edge->getTarget();
                if (next_target_object) {
                    deque<int> new_result = next_target_object->collect_blue(edgelist);
                    if (new_result.size() > 0) {
                        for_each( new_result.begin(),
                                  new_result.end(),
                                  [&result] (int& n) { result.push_back(n); } );
                    }
                    pair<int,int> newedge(this->getId(), next_target_object->getId());
                    edgelist.push_back( newedge );
                    // NOTE: this may add an edge that isn't in the cyclic garbage.
                    // These invalid edges will be filtered out later when
                    // we know for sure what the cyclic component is.
                }
            }
        }
    }
    return result;
}

void Object::makeDead(unsigned int death_time)
{
    // -- Record the death time
    this->m_deathTime = death_time;

    // -- Visit all edges
    for ( EdgeMap::iterator p = this->m_fields.begin();
          p != this->m_fields.end();
          p++ ) {
        Edge* edge = p->second;

        if (edge) {
            // -- Edge dies now
            edge->setEndTime(death_time);
        }
    }

    if (HeapState::debug) {
        cout << "Dead object " << m_id << " of type " << m_type << endl;
    }
}

void Object::recolor( Color newColor )
{
    // Maintain the invariant that the reference count of a node is
    // the number of GREEN or BLACK pointers to it.
    for ( EdgeMap::iterator p = this->m_fields.begin();
          p != this->m_fields.end();
          p++ ) {
        Edge* edge = p->second;
        if (edge) {
            Object* target = edge->getTarget();
            if (target) {
                if ( ((this->m_color == GREEN) || (this->m_color == BLACK)) &&
                     ((newColor != GREEN) && (newColor != BLACK)) ) {
                    // decrement reference count of target
                    target->decrementRefCount();
                } else if ( ((this->m_color != GREEN) && (this->m_color != BLACK)) &&
                            ((newColor == GREEN) || (newColor == BLACK)) ) {
                    // increment reference count of target
                    target->incrementRefCountReal();
                }
            }
        }
    }
    this->m_color = newColor;
}

void Object::decrementRefCountReal( unsigned int cur_time, Method *method )
{
    this->decrementRefCount();
    this->m_lastMethodDecRC = method;
    if (this->m_refCount == 0) {
        // TODO Should we even bother with this check?
        //      Maybe just set it to true.
        if (!m_decToZero) {
            m_decToZero = true;
            m_methodRCtoZero = method;
            this->g_counter++;
        }
        // -- Visit all edges
        this->recolor(GREEN);
        for ( EdgeMap::iterator p = this->m_fields.begin();
              p != this->m_fields.end();
              ++p ) {
            Edge* target_edge = p->second;
            if (target_edge) {
                unsigned int fieldId = target_edge->getSourceField();
                this->updateField( NULL, fieldId, cur_time, method );
            }
        }
        // DEBUG
        // if (Object::g_counter % 1000 == 1) {
        // cout << ".";
        // }
    } else {
        Color color = this->getColor();
        if (color != BLACK) {
            unsigned int objId = this->getId();
            this->recolor( BLACK );
            this->m_heapptr->set_candidate(objId);
        }
    }
}

void Object::incrementRefCountReal()
{
    if ((this->m_refCount == 0) && this->m_decToZero) {
        this->m_incFromZero = true;
        this->m_methodRCtoZero = NULL;
        cout << "X";
    }
    this->incrementRefCount();
    // TODO
    // Can we take it out of the candidate set? If so, what should
    // the new color be?
    // {
    //     Color color = this->getColor();
    //     if (color != BLACK) {
    //         unsigned int objId = this->getId();
    //         this->recolor(BLACK);
    //         this->m_heapptr->set_candidate(objId);
    //     }
    // }
}

