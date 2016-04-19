#include "heap.h"

// -- Global flags
bool HeapState::do_refcounting = true;
bool HeapState::debug = false;
unsigned int Object::g_counter = 0;

Object* HeapState::allocate( unsigned int id,
                             unsigned int size,
                             char kind,
                             char* type,
                             AllocSite* site, 
                             unsigned int els,
                             Thread* thread,
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
    unsigned long int temp = this->m_liveSize + obj->getSize();
    // Max live size calculation
    this->m_liveSize = ( (temp < this->m_liveSize) ? ULONG_MAX : temp );
    if (this->m_maxLiveSize < this->m_liveSize) {
        this->m_maxLiveSize = this->m_liveSize;
    }
    return obj;
}

// -- Manage heap
Object* HeapState::getObject(unsigned int id)
{
    ObjectMap::iterator p = m_objects.find(id);
    if (p != m_objects.end()) {
        return (*p).second;
    }
    else {
        return 0;
    }
}

Edge* HeapState::make_edge( Object* source,
                            FieldId_t field_id,
                            Object* target,
                            unsigned int cur_time )
{
    Edge* new_edge = new Edge( source, field_id,
                               target, cur_time );
    m_edges.insert(new_edge);
    assert(target != NULL);

    if (m_edges.size() % 100000 == 0) {
        cout << "EDGES: " << m_edges.size() << endl;
    }

    return new_edge;
}

void HeapState::makeDead(Object * obj, unsigned int death_time)
{
    unsigned long int temp = this->m_liveSize - obj->getSize();
    if (temp > this->m_liveSize) {
        // OVERFLOW, underflow?
        this->m_liveSize = 0;
        cerr << "UNDERFLOW of substraction." << endl;
        // TODO If this happens, maybe we should think about why it happens.
    } else {
        // All good. Fight on.
        this->m_liveSize = temp;
    }
    obj->makeDead(death_time);
}

// TODO Documentation :)
void HeapState::update_death_counters( Object *obj )
{
    // VM type objects
    if (obj->getKind() == 'V') {
        if (obj->getRefCount() == 0) {
            m_vm_refcount_0++;
        } else {
            m_vm_refcount_positive++;
        }
    }
}

// TODO Documentation :)
void HeapState::end_of_program(unsigned int cur_time)
{
    // -- Set death time of all remaining live objects
    //    Also set the flags for the interesting classifications.
    for ( ObjectMap::iterator i = m_objects.begin();
          i != m_objects.end();
          ++i ) {
        Object* obj = i->second;
        if (obj->isLive(cur_time)) {
            // Go ahead and ignore the call to HeapState::makeDead
            // as we won't need to update maxLiveSize here anymore.
            obj->makeDead(cur_time);
        }
        // Do the count of heap vs stack loss here.
        this->update_death_counters(obj);
    }
}

// TODO Documentation :)
void HeapState::set_reason_for_cycles( deque< deque<int> >& cycles )
{
    for ( deque< deque<int> >::iterator it = cycles.begin();
          it != cycles.end();
          ++it ) {
        Reason reason = UNKNOWN_REASON;
        unsigned int last_action_time = 0;
        for ( deque<int>::iterator objit = it->begin();
              objit != it->end();
              ++objit ) {
            Object* object = this->getObject(*objit);
            unsigned int objtime = object->getLastActionTime();
            if (objtime > last_action_time) {
                reason = object->getReason();
                last_action_time = objtime;
            }
        }
        for ( deque<int>::iterator objit = it->begin();
              objit != it->end();
              ++objit ) {
            Object* object = this->getObject(*objit);
            object->setReason( reason, last_action_time );
        }
    }
}

NodeId_t HeapState::getNodeId( ObjectId_t objId, bimap< ObjectId_t, NodeId_t >& bmap ) {
    bimap< ObjectId_t, NodeId_t >::left_map::const_iterator liter = bmap.left.find(objId);
    if (liter == bmap.left.end()) {
        // Haven't mapped a NodeId yet to this ObjectId
        NodeId_t nodeId = bmap.size();
        bmap.insert( bimap< ObjectId_t, NodeId_t >::value_type( objId, nodeId ) );
        return nodeId;
    } else {
        // We have a NodeId
        return liter->second;
    }
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

string Object::info2() {
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
       << ")"
       << " : " << (m_deathTime - m_createTime);
    return ss.str();
}

void Object::updateField( Edge* edge,
                          FieldId_t fieldId,
                          unsigned int cur_time,
                          Method *method,
                          Reason reason )
{
    EdgeMap::iterator p = this->m_fields.find(fieldId);
    if (p != this->m_fields.end()) {
        // -- Old edge
        Edge* old_edge = p->second;
        if (old_edge) {
            // -- Now we know the end time
            Object *old_target = old_edge->getTarget();
            if (old_target) {
                old_target->decrementRefCountReal( cur_time, method, reason );
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
    if (this->m_deadFlag) {
        cerr << "Object[ " << this->getId() << " ] : double Death event." << endl;
    } else {
        this->m_deadFlag = true;
    }

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

void Object::decrementRefCountReal( unsigned int cur_time,
                                    Method *method,
                                    Reason reason )
{
    this->decrementRefCount();
    this->m_lastMethodDecRC = method;
    if (reason == STACK) {
        this->setLastEvent( LastEvent::ROOT );
    } else if (reason == HEAP) {
        this->setLastEvent( LastEvent::UPDATE);
    }
    if (this->m_refCount == 0) {
        if (!m_decToZero) {
            m_decToZero = true;
            m_methodRCtoZero = method;
            this->g_counter++;
        }
        // -- Visit all edges
        this->recolor(GREEN);

        // Edges are now dead.
        for ( EdgeMap::iterator p = this->m_fields.begin();
              p != this->m_fields.end();
              ++p ) {
            Edge* target_edge = p->second;
            if (target_edge) {
                unsigned int fieldId = target_edge->getSourceField();
                this->updateField( NULL,
                                   fieldId,
                                   cur_time,
                                   method,
                                   reason );
            }
        }
    }
}

void Object::incrementRefCountReal()
{
    if ((this->m_refCount == 0) && this->m_decToZero) {
        this->m_incFromZero = true;
        this->m_methodRCtoZero = NULL;
    }
    this->incrementRefCount();
    this->m_maxRefCount = std::max( m_refCount, m_maxRefCount );
}

