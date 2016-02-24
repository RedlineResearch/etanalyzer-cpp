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
    unsigned long int temp = this->m_liveSize + obj->getSize();
    this->m_liveSize = ( (temp < this->m_liveSize) ? ULONG_MAX : temp);
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
    unsigned int obj_size = obj->getSize();
    // VERSION 1
    // TODO: This could use some refactoring.
    if ( obj->getDiedByStackFlag() ||
         ( ((obj->getReason() == STACK) ||
            (obj->getLastEvent() == LastEvent::ROOT)) &&
            !obj->getDiedByHeapFlag() )
         ) {
        if (m_obj_debug_flag) {
            cout << "S> " << obj->info2();
        }
        this->m_totalDiedByStack_ver2++;
        this->m_sizeDiedByStack += obj_size;
        if (obj->wasPointedAtByHeap()) {
            this->m_diedByStackAfterHeap++;
            this->m_diedByStackAfterHeap_size += obj_size;
            if (m_obj_debug_flag) {
                cout << " AH>" << endl;
            }
        } else {
            this->m_diedByStackOnly++;
            this->m_diedByStackOnly_size += obj_size;
            if (m_obj_debug_flag) {
                cout << " SO>" << endl;
            }
        }
        if (obj->wasLastUpdateNull()) {
            this->m_totalUpdateNullStack++;
            this->m_totalUpdateNullStack_size += obj_size;
        }
    } else if ( obj->getDiedByHeapFlag() ||
                (obj->getReason() == HEAP) ||
                (obj->getLastEvent() == LastEvent::UPDATE) ||
                obj->wasPointedAtByHeap() ) {
        if (m_obj_debug_flag) {
            cout << "H> " << obj->info2();
        }
        this->m_totalDiedByHeap_ver2++;
        this->m_sizeDiedByHeap += obj_size;
        obj->setDiedByHeapFlag();
        if (obj->wasLastUpdateNull()) {
            this->m_totalUpdateNullHeap++;
            this->m_totalUpdateNullHeap_size += obj_size;
            if (m_obj_debug_flag) {
                cout << " NL>" << endl;
            } else {
                cout << " VA>" << endl;
            }
        }
    } else {
        // cout << "X: ObjectID [" << obj->getId() << "][" << obj->getType()
        //      << "] RC = " << obj->getRefCount() << " maxRC: " << obj->getMaxRefCount()
        //      << " Atype: " << obj->getKind() << endl;
        // All these objects were never a target of an Update event. For example,
        // most VM allocated objects (by event V) are never targeted by
        // the Java user program, and thus end up here. We consider these
        // to be "STACK" caused death as we can associate these with the main function.
        if (m_obj_debug_flag) {
            cout << "S> " << obj->info2() << " SO>" << endl;
        }
        this->m_totalDiedByStack_ver2++;
        this->m_sizeDiedByStack += obj_size;
        this->m_diedByStackOnly++;
        this->m_diedByStackOnly_size += obj_size;
        if (obj->wasLastUpdateNull()) {
            this->m_totalUpdateNullStack++;
            this->m_totalUpdateNullStack_size += obj_size;
        }
    }
    if (obj->wasLastUpdateNull()) {
        this->m_totalUpdateNull++;
        this->m_totalUpdateNull_size += obj_size;
    }
    // END VERSION 1
    
    // VM type objects
    if (obj->getKind() == 'V') {
        if (obj->getRefCount() == 0) {
            m_vm_refcount_0++;
        } else {
            m_vm_refcount_positive++;
        }
    }
}

Method * HeapState::get_method_death_site( Object *obj )
{
    Method *dsite = obj->getDeathSite();
    if (obj->getDiedByHeapFlag()) {
        // DIED BY HEAP
        if (!dsite) {
            if (obj->wasDecrementedToZero()) {
                // So died by heap but no saved death site. First alternative is
                // to look for the a site that decremented to 0.
                dsite = obj->getMethodDecToZero();
            } else {
                // TODO: No dsite here yet
                // TODO TODO TODO
                // This probably should be the garbage cycles. Question is 
                // where should we get this?
            }
        }
    } else {
        if (obj->getDiedByStackFlag()) {
            // DIED BY STACK.
            //   Look for last heap activity.
            dsite = obj->getLastMethodDecRC();
        }
    }
    return dsite;
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
            if (obj->getReason() == HEAP) {
                obj->setDiedByHeapFlag();
            } else {
                obj->setDiedByStackFlag();
            }
            obj->setLastEvent( LastEvent::ROOT );
        }
        // Do the count of heap vs stack loss here.
        this->update_death_counters(obj);

        // Save method death site to map
        Method *dsite = this->get_method_death_site( obj );

        // Process the death sites
        if (dsite) {
            DeathSitesMap::iterator it = this->m_death_sites_map.find(dsite);
            if (it == this->m_death_sites_map.end()) {
                this->m_death_sites_map[dsite] = new set<string>; 
            }
            this->m_death_sites_map[dsite]->insert(obj->getType());
        } else {
            this->m_no_dsites_count++;
            // TODO if (obj->getDiedByHeapFlag()) {
            //     // We couldn't find a deathsite for something that died by heap.
            //     // TODO ?????? TODO
            // } else if (obj->getDiedByStackFlag()) {
            //     // ?
            // } else {
            // }
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
            Object* object = this->getObject(objId);
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
    this->set_reason_for_cycles( result );
    return result;
}

NodeId_t HeapState::getNodeId( ObjectId_t objId, GraphBiMap_t& bmap ) {
    GraphBiMap_t::left_map::const_iterator liter = bmap.left.find(objId);
    if (liter == bmap.left.end()) {
        // Haven't mapped a NodeId yet to this ObjectId
        NodeId_t nodeId = bmap.size();
        bmap.insert( GraphBiMap_t::value_type( objId, nodeId ) );
        return nodeId;
    } else {
        // We have a NodeId
        return liter->second;
    }
}

// TODO Documentation :)
void HeapState::scan_queue2( EdgeList& edgelist,
                                  map<unsigned int, bool>& not_candidate_map,
                                  GraphBiMap_t& bmap,
                                  KeySet_t& keyset )
{
    unsigned int hit_total;
    unsigned int miss_total;
    // keyset contains:
    //   key object objects as keys
    //   sets of objects that depend on key objects
    cout << "Queue size: " << this->m_candidate_map.size() << endl;
    // TODO
    // 1. Convert m_candidate_map to a Boost Graph Library
    // 2. Run SCC algorithm
    // 3. Run reachability from the SCCs to the rest
    // 4. ??? That's it?
    //
    // TODO: Add bimap
    //    objId <-> graph ID
    //
    for ( map<unsigned int, bool>::iterator i = this->m_candidate_map.begin();
          i != this->m_candidate_map.end();
          ++i ) {
        ObjectId_t objId = i->first;
        NodeId_t srcNodeId;
        bool flag = i->second;
        if (flag) {
            // Is a candidate
            Object *obj = this->getObject(objId);
            if ( obj && (obj->getRefCount() > 0) ) {
                // Object exists
                unsigned int dtime = obj->getDeathTime();
                srcNodeId = getNodeId(objId, bmap);
                this->m_candidate_set.insert( std::make_pair( objId, dtime ) );
                for ( EdgeMap::iterator p = obj->getEdgeMapBegin();
                      p != obj->getEdgeMapEnd();
                      ++p ) {
                    Edge* target_edge = p->second;
                    if (target_edge) {
                        unsigned int fieldId = target_edge->getSourceField();
                        Object *tgtObj = target_edge->getTarget();
                        if (tgtObj) {
                            ObjectId_t tgtId = tgtObj->getId();
                            NodeId_t tgtNodeId = getNodeId(tgtId, bmap);
                            // TODO GEdge_t e(srcNodeId, tgtNodeId);
                            GEdge_t e(objId, tgtId);
                            edgelist.push_back(e);
                        }
                    }
                }
            }
        } // if (flag)
    }
    cout << "bmap size: " << bmap.size() << endl;
    while (!(this->m_candidate_set.empty())) {
        std::set<pair< unsigned int, unsigned int >>::iterator it = this->m_candidate_set.begin();
        if (it != this->m_candidate_set.end()) {
            unsigned int rootId = it->first;
            unsigned int last_update_time = it->second;
            Object *root = this->getObject(rootId);
            // DFS work stack - can't use 'stack' as a variable name
            std::deque< Object * > work;
            // The discovered set of objects.
            std::set< Object * > discovered;
            // Root goes in first.
            work.push_back(root);
            keyset[root] = new std::set< Object * >();
            keyset[root]->insert( root );
            // Remove from candidate set.
            this->m_candidate_set.erase(it);
            // Depth First Search
            while (!work.empty()) {
                Object *src = work.back();
                work.pop_back();
                std::set< Object * >::iterator it = discovered.find(src);
                if (it == discovered.end()) {
                    // Not yet seen.
                    discovered.insert(src);
                    keyset[root]->insert(src);
                    for ( EdgeMap::iterator p = src->getEdgeMapBegin();
                          p != src->getEdgeMapEnd();
                          ++p ) {
                        Edge* target_edge = p->second;
                        if (target_edge) {
                            Object *tgtObj = target_edge->getTarget();
                            work.push_back(tgtObj);
                        }
                    }
                }
            }
        }
    }
    cout << endl;
    cout << "  MISSES: " << miss_total << "   HITS: " << hit_total << endl;
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
                          unsigned int fieldId,
                          unsigned int cur_time,
                          Method *method,
                          Reason reason,
                          ObjectId_t death_root )
{
    EdgeMap::iterator p = this->m_fields.find(fieldId);
    if (p != this->m_fields.end()) {
        // -- Old edge
        Edge* old_edge = p->second;
        if (old_edge) {
            // -- Now we know the end time
            Object *old_target = old_edge->getTarget();
            if (old_target) {
                if (reason == HEAP) {
                    old_target->setHeapReason( cur_time );
                } else if (reason == STACK) {
                    old_target->setStackReason( cur_time );
                }
                old_target->decrementRefCountReal(cur_time, method, reason, death_root);
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

void Object::decrementRefCountReal( unsigned int cur_time,
                                    Method *method,
                                    Reason reason,
                                    ObjectId_t death_root )
{
    this->decrementRefCount();
    this->m_lastMethodDecRC = method;
    if (reason == STACK) {
        this->setLastEvent( LastEvent::ROOT );
    } else if (reason == HEAP) {
        this->setLastEvent( LastEvent::UPDATE);
    }
    if (this->m_refCount == 0) {
        // TODO Should we even bother with this check?
        //      Maybe just set it to true.
        if (!m_decToZero) {
            m_decToZero = true;
            m_methodRCtoZero = method;
            this->g_counter++;
        }
        if (reason == STACK) {
            this->setDiedByStackFlag();
        } else {
            this->setDiedByHeapFlag();
        }
        // -- Visit all edges
        this->recolor(GREEN);
        // -- Who's my key object?
        this->m_death_root = death_root;
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
                                   reason,
                                   death_root );
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
    }
    this->incrementRefCount();
    this->m_maxRefCount = std::max( m_refCount, m_maxRefCount );
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

