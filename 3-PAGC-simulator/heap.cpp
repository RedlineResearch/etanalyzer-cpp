#include "heap.h"

// -- Global flags
bool HeapState::do_refcounting = true;
bool HeapState::debug = false;
unsigned int Object::g_counter = 0;

bool HeapState::initialize_memory_basic( unsigned long int heapsize )
{
    // Initialize the BASIC memory manager
    this->m_memmgr_p = new MemoryMgr();
    bool result = this->m_memmgr_p->initialize_memory( heapsize );
    return result;
}

bool HeapState::initialize_memory_deferred( unsigned long int heapsize,
                                            string &group_filename,
                                            int numgroups )
{
    cout << "initialize_memory_deferred:" << endl;
    // Initialize the BASIC memory manager
    this->m_memmgrdef_p = new MemoryMgrDef();
    this->m_memmgr_p = static_cast<MemoryMgr *>(this->m_memmgrdef_p);
    bool result = this->m_memmgrdef_p->initialize_memory( heapsize );
    if (!result) {
        return false;
    }
    result = this->m_memmgrdef_p->initialize_special_group( group_filename, numgroups );
    return result;
}

// Initialize memory for the GC simulator
//    - heapsize in bytes
//    - group_filename is the group2list.csv file output from dgroups2db.py
//    - numgroups to include (right now this can only be one)
bool HeapState::initialize_memory_deferred_VER2( unsigned long heapsize,
                                                 string &group_filename,
                                                 int numgroups )
{
    cout << "initialize_memory_deferred:" << endl;
    // Initialize the BASIC memory manager
    this->m_memmgrdef_p = new MemoryMgrDef();
    this->m_memmgr_p = static_cast<MemoryMgr *>(this->m_memmgrdef_p);
    bool result = this->m_memmgrdef_p->initialize_memory( heapsize );
    if (!result) {
        return false;
    }
    result = this->m_memmgrdef_p->initialize_special_group( group_filename, numgroups );
    return result;
}

// Initialize memory for the Program Aware GC simulator
//    - heapsize in bytes
bool HeapState::initialize_memory_PAGC_VER1( unsigned long heapsize,
                                             string &PAGC_config_filename )
{
    cout << "initialize_memory_PAGC_VER1:" << endl;
    // Initialize the BASIC memory manager
    this->m_memmgr_PAGC_VER1_p = new MemoryMgrDef();
    this->m_memmgr_p = static_cast<MemoryMgr *>(this->m_memmgr_PAGC_VER1_p);
    bool result = this->m_memmgr_PAGC_VER1_p->initialize_memory( heapsize );
    if (!result) {
        return false;
    }
    // TODO: result = this->m_memmgr_PAGC_VER1_p->initialize_special_group( group_filename, numgroups );
    return result;
}

// - TODO
Object* HeapState::allocate( unsigned int id,
                             unsigned int size,
                             char kind,
                             char *type,
                             AllocSite *site, 
                             unsigned int els,
                             Thread *thread,
                             unsigned int create_time )
{
    // Design decision: allocation time isn't 0 based.
    this->m_alloc_time += size;
    Object* obj = new Object( id,
                              size,
                              kind,
                              type,
                              site,
                              els,
                              thread,
                              create_time,
                              this );
    m_objects[obj->getId()] = obj;

    // Call the Memory Manager allocate
    bool success = this->m_memmgr_p->allocate( obj, create_time, this->getAllocTime() );
    if (!success) {
        cout << "OOM Error:"
             << " ObjId: " << obj->getId()
             << " type: " << obj->getType()
             << " size: " << obj->getSize() << endl;
        exit(1);
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
    target->setPointedAtByHeap();

    if (m_edges.size() % 100000 == 0) {
        cout << "EDGES: " << m_edges.size() << endl;
    }

    return new_edge;
}

// - TODO
void HeapState::make_edge2( unsigned int objId, unsigned int tgtId )
{
    this->m_memmgr_p->add_edge( objId, tgtId );
}

// - TODO
void HeapState::remove_edge2( unsigned int objId, unsigned int oldTgtId )
{
    this->m_memmgr_p->remove_edge( objId, oldTgtId );
    this->m_memmgr_p->remove_from_srcidmap( objId,
                                         oldTgtId );
    this->m_memmgr_p->remove_from_tgtidmap( objId,
                                         oldTgtId );
}


void HeapState::makeDead(Object * obj, unsigned int death_time)
{
    if (this->m_memmgr_p->is_in_live_set(obj)) {
        this->m_memmgr_p->makeDead( obj, death_time );
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
    // We don't really care to clean up anything at the end of the program.
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
                          Reason reason,
                          Object *death_root )
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
                } else {
                    cerr << "Invalid reason." << endl;
                    assert( false );
                }
                old_target->decrementRefCountReal( cur_time,
                                                   method,
                                                   reason,
                                                   death_root );
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

void Object::makeDead( unsigned int death_time,
                       unsigned int death_time_alloc )
{
    // -- Record the death time
    this->m_deathTime = death_time;
    this->m_deathTime_alloc = death_time_alloc;
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
                                    Reason reason,
                                    Object *death_root )
{
    this->decrementRefCount();
    this->m_lastMethodDecRC = method;
    if (this->m_refCount == 0) {
        ObjectPtrMap_t& whereis = this->m_heapptr->get_whereis();
        KeySet_t& keyset = this->m_heapptr->get_keyset();
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
        if (death_root != NULL) {
            this->setDeathRoot( death_root );
        } else {
            this->setDeathRoot( this );
        }
        Object *my_death_root = this->getDeathRoot();
        if (!my_death_root) {
            my_death_root = this;
        }
        whereis[this] = my_death_root;
        KeySet_t::iterator itset = keyset.find(my_death_root);
        if (itset == keyset.end()) {
            keyset[my_death_root] = new std::set< Object * >();
            keyset[my_death_root]->insert( my_death_root );
        }
        keyset[my_death_root]->insert( this );

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
                                   reason,
                                   this->getDeathRoot() );
            }
        }
        // DEBUG
        // if (Object::g_counter % 1000 == 1) {
        // cout << ".";
        // }
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

