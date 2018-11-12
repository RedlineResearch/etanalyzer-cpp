#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <map>
#include <set>
#include <vector>
#include <deque>
#include <string>
#include <utility>
// TODO #include <stxxl/map>

using namespace std;

#include "tokenizer.h"
#include "classinfo.h"
#include "execution.h"
#include "heap.h"
#include "refstate.h"
#include "summary.hpp"
// #include "lastmap.h"
#include "version.hpp"

// // STXXL related constants
// #define DATA_NODE_BLOCK_SIZE (4096)
// #define DATA_LEAF_BLOCK_SIZE (4096)

// ----------------------------------------------------------------------
// Types
class Object;
class CCNode;

typedef std::map< string, std::vector< Summary * > > GroupSum_t;
typedef std::map< string, Summary * > TypeTotalSum_t;
typedef std::map< unsigned int, Summary * > SizeSum_t;

// An Edge Source is an object and it's specific field.
// Global edge sources have a NULL object pointer.
typedef std::pair< Object *, FieldId_t > EdgeSrc_t; 
// ----------------------------------------------------------------------
// We then map Edge Sources to a vector/list of Object pointers.
// For every edge source, we keep a list of all the objects (pointers)
// it has ever pointed to.
// This is the in-memory stl version
typedef std::map< EdgeSrc_t, std::vector< Object * > > EdgeSummary_t;
// We need a reverse map from object pointer to the edge sources that ever
// pointed to that object.
typedef std::map< Object *, std::vector< EdgeSrc_t > > Object2EdgeSrcMap_t;

// Trying an on-disk version but the library I DLed doesn't work well with C++11
// TODO //! [comparator]
// TODO struct CompareGreater_EdgeSummary
// TODO {
// TODO     bool operator () (const EdgeSrc_t &a, const EdgeSrc_t &b) const {
// TODO         Object *first = std::get<0>(a); 
// TODO         Object *second = std::get<0>(b); 
// TODO         if (first != second) {
// TODO             return first > second;
// TODO         } else {
// TODO             FieldId_t field1 = std::get<1>(a); 
// TODO             FieldId_t field2 = std::get<1>(b); 
// TODO             return field1 > field2;
// TODO         }
// TODO     }
// TODO     static EdgeSrc_t max_value() {
// TODO         return std::make_pair( static_cast<Object *>(NULL),
// TODO                                std::numeric_limits<unsigned int>::min() );
// TODO     }
// TODO };
// TODO //! [comparator]
// TODO // This is the out-of-core (on-disk) version.
// TODO typedef stxxl::map< EdgeSrc_t, std::vector< Object * >,
// TODO                     CompareGreater_EdgeSummary,
// TODO                     DATA_NODE_BLOCK_SIZE, DATA_LEAF_BLOCK_SIZE >
// TODO     stxxl_EdgeSummary_t;
// TODO // TODO: This is just dummy code to test the STXXL compilation units.
// TODO // Need to replace EdgeSummary_t with the stxxl::map. (Or maybe make it configurable)
// TODO // TODO: Test
// TODO typedef stxxl_EdgeSummary_t EdgeSummary_t;
// TODO 
// TODO //! [comparator]
// TODO struct CompareGreater
// TODO {
// TODO     bool operator () (const int & a, const int & b) const {
// TODO         return a > b;
// TODO     }
// TODO     static int max_value() {
// TODO         return std::numeric_limits<int>::min();
// TODO     }
// TODO };
// TODO //! [comparator]
// TODO // This is the out-of-core (on-disk) version.
// TODO //! [comparator]
// TODO typedef stxxl::map<int, char, CompareGreater, DATA_NODE_BLOCK_SIZE, DATA_LEAF_BLOCK_SIZE> stxxl_Object2EdgeSrcMap_t;


// EdgeSummary_t and Object2EdgeSrcMap_t contain the raw data. We need some
// summaries.
// enum class ObjectRefType moved to heap.h - RLV

enum class EdgeSrcType {
    STABLE = 1, // Like True Love. Only one target ever.
    // TODO: This is an original idea that maybe singly-owned objects
    // can be considered part of the stable group.
    SERIAL_STABLE = 2, // Kind of like a serial monogamist.
                       // Can point to different objects but never shares the
                       // target object
    UNSTABLE = 32, // Points to different objects and
                   // shares objects with other references
    UNKNOWN = 1024
};

string edgesrctype2str( EdgeSrcType r)
{
    if (r == EdgeSrcType::STABLE) {
        return "STABLE";
    } else if (r == EdgeSrcType::SERIAL_STABLE) {
        return "SERIAL_STABLE";
    } else if (r == EdgeSrcType::UNSTABLE) {
        return "UNSTABLE";
    } else {
        return "UNKNOWN";
    }
}

string edgesrctype2shortstr( EdgeSrcType r)
{
    if (r == EdgeSrcType::STABLE) {
        return "S"; // STABLE
    } else if (r == EdgeSrcType::SERIAL_STABLE) {
        return "ST"; // SERIAL_STABLE
    } else if (r == EdgeSrcType::UNSTABLE) {
        return "U"; // UNSTABLE
    } else {
        return "X"; // UNKNOWN
    }
}

// Map a reference to its type
typedef std::map< EdgeSrc_t, EdgeSrcType > EdgeSrc2Type_t;
// Map an object to its type
// TODO: Is this needed?
typedef std::map< Object *, ObjectRefType > Object2Type_t;

// ----------------------------------------------------------------------
//   Globals

// -- Key object map to set of objects
KeySet_t keyset;
// -- Object to key object map
ObjectPtrMap_t whereis;

// -- The heap
HeapState Heap( whereis, keyset );

// -- Execution state
#ifdef ENABLE_TYPE1
ExecMode cckind = ExecMode::Full; // Full calling context
#else
ExecMode cckind = ExecMode::StackOnly; // Stack-only context
#endif // ENABLE_TYPE1

ExecState Exec(cckind);

// -- Turn on debugging
bool debug = false;

// -- Remember the last event by thread ID
// TODO: Types for object and thread IDs?
// TODO LastMap last_map;

// ----------------------------------------------------------------------
//   Analysis
deque< deque<Object*> > cycle_list;
set<unsigned int> root_set;

map<unsigned int, unsigned int> deathrc_map;
map<unsigned int, bool> not_candidate_map;

EdgeSummary_t edge_summary;
Object2EdgeSrcMap_t obj2ref_map;


// ----------------------------------------------------------------------
void sanity_check()
{
/*
   if (Now > obj->getDeathTime() && obj->getRefCount() != 0) {
   nonzero_ref++;
   printf(" Non-zero-ref-dead %X of type %s\n", obj->getId(), obj->getType().c_str());
   }
   */
}

bool member( Object* obj, const ObjectSet& theset )
{
    return theset.find(obj) != theset.end();
}


unsigned int count_live( ObjectSet & objects,
                         unsigned int at_time )
{
    int count = 0;
    // -- How many are actually live
    for ( auto p = objects.begin();
          p != objects.end();
          p++ ) {
        Object* obj = *p;
        if (obj->isLive(at_time)) {
            count++;
        }
    }

    return count;
}

void update_reference_summaries( Object *src,
                                 FieldId_t fieldId,
                                 Object *tgt )
{
    // edge_summary : EdgeSummary_t is a global
    // obj2ref_map : Object2EdgeSrcMap_t is a global
    EdgeSrc_t ref = std::make_pair( src, fieldId );
    // The reference 'ref' points to the new target
    auto iter = edge_summary.find(ref);
    if (iter == edge_summary.end()) {
        // Not found. Create a new vector of Object pointers
        edge_summary[ref] = std::vector< Object * >();
    }
    edge_summary[ref].push_back(tgt);
    // Do the reverse mapping ONLY if tgt is not NULL
    if (tgt) {
        auto rev = obj2ref_map.find(tgt);
        if (rev == obj2ref_map.end()) {
            obj2ref_map[tgt] = std::move(std::vector< EdgeSrc_t >());
        }
        obj2ref_map[tgt].push_back(ref);
    }
}

// ----------------------------------------------------------------------
//   Read and process trace events
void apply_merlin( std::deque< Object * > &new_garbage )
{
    if (new_garbage.size() == 0) { 
        // TODO Log an ERROR or a WARNING
        return;
    }
    // TODO: Use setDeathTime( new_dtime );
    // Sort the new_garbage vector according to latest last_timestamp.
    std::sort( new_garbage.begin(),
               new_garbage.end(),
               []( Object *left, Object *right ) {
                   return (left->getLastTimestamp() > right->getLastTimestamp());
               } );
    // Do a standard DFS.
    while (new_garbage.size() > 0) {
        // Start with the latest for the DFS.
        Object *cur = new_garbage[0];
        new_garbage.pop_front();
        std::deque< Object * > mystack; // stack for DFS
        std::set< Object * > labeled; // What Objects have been labeled
        mystack.push_front( cur );
        while (mystack.size() > 0) {
            Object *otmp = mystack[0];
            mystack.pop_front();
            assert(otmp);
            // The current timestamp max
            unsigned int tstamp_max = otmp->getLastTimestamp();
            otmp->setDeathTime( tstamp_max );
            // Remove from new_garbage deque
            auto ngiter = std::find( new_garbage.begin(),
                                     new_garbage.end(),
                                     otmp );
            if (ngiter != new_garbage.end()) {
                // still in the new_garbage deque
                new_garbage.erase(ngiter);
            }
            // Check if labeled already
            auto iter = labeled.find(otmp);
            if (iter == labeled.end()) {
                // Not found => not labeled

                labeled.insert(otmp);
                // Go through all the edges out of otmp
                // -- Visit all edges
                for ( auto ptr = otmp->getFields().begin();
                      ptr != otmp->getFields().end();
                      ptr++ ) {
                    Edge *edge = ptr->second;
                    if ( edge &&
                         ((edge->getEdgeState() == EdgeState::DEAD_BY_OBJECT_DEATH) ||
                           (edge->getEdgeState() == EdgeState::DEAD_BY_OBJECT_DEATH_NOT_SAVED)) ) {
                        edge->setEndTime( tstamp_max );
                        Object *mytgt = edge->getTarget();
                        if (mytgt) {
                            // Get timestamp
                            unsigned int tstmp = mytgt->getLastTimestamp();
                            if (tstamp_max > tstmp) {
                                mytgt->setLastTimestamp( tstamp_max );
                                mytgt->setDeathTime( tstamp_max );
                            }
                            // This might be a bug. I probably shouldn't be setting the tstamp_max here.
                            // But then again where should it be set? It seems that this is the right thing
                            // to do here.
                            // TODO else {
                            // TODO     tstamp_max = tstmp;
                            // TODO }
                            mystack.push_front( mytgt );
                        }
                    }
                }
            }
        }
    }
    // Until the vector is empty
}

// ----------------------------------------------------------------------
//   Read and process trace events
unsigned int read_trace_file( FILE *f )
{
    Tokenizer tokenizer(f);

    unsigned int method_id;
    unsigned int object_id;
    unsigned int target_id;
    unsigned int field_id;
    unsigned int thread_id;
    unsigned int exception_id;
    Object *obj;
    Object *target;
    Method *method;
    unsigned int total_objects = 0;

    // Remember all the dead objects
    std::deque< Object * > new_garbage;
    Method *main_method = ClassInfo::get_main_method();
    unsigned int main_id = (main_method != NULL ? main_method->getId() : 0);

    unsigned int latest_death_time = 0;

    // DEBUG
    unsigned int debug_stack_edges = 0;
    // END DEBUG
    // -- Allocation time
    unsigned int AllocationTime = 0;
    while ( ! tokenizer.isDone()) {
        tokenizer.getLine();
        if (tokenizer.isDone()) {
            break;
        }

        if (Exec.NowUp() % 1050000 == 1) {
            // cout << "  Method time: " << Exec.Now() << "   Alloc time: " << AllocationTime << endl;
            cout << "  Update time: " << Exec.NowUp() << " | Method time: TODO | Alloc time: " << AllocationTime << endl;
        }

        switch (tokenizer.getChar(0)) {
            case 'A':
            case 'I':
            case 'N':
            case 'P':
            case 'V':
                {
                    // A/I/N/P/V <id> <size> <type> <site> [<els>] <threadid>
                    //     0       1    2      3      4      5         5/6
                    unsigned int thrdid = (tokenizer.numTokens() == 6) ? tokenizer.getInt(6)
                                                                       : tokenizer.getInt(5);
                    Thread* thread = Exec.getThread(thrdid);
                    unsigned int els  = (tokenizer.numTokens() == 6) ? tokenizer.getInt(5)
                                                                     : 0;
                    AllocSite *as = ClassInfo::TheAllocSites[tokenizer.getInt(4)];
                    assert(thread);
                    // TODO: // Get context pair
                    // TODO: ContextPair cpair = thread->getContextPair();
                    // TODO: CPairType cptype = thread->getContextPairType();
                    // DEBUG
                    // if (!as) {
                    //     cerr << "DBG: objId[ " << tokenizer.getInt(1) << " ] has no alloc site." << endl;
                    // } // END DEBUG
                    Heap.incAllocTime( tokenizer.getInt(2) ); // size
                    // TODO DELETE MAYBE obj = Heap.allocate( tokenizer.getInt(1),    // id
                    // TODO DELETE MAYBE                      tokenizer.getInt(2),    // size
                    // TODO DELETE MAYBE                      tokenizer.getChar(0),   // kind of alloc
                    // TODO DELETE MAYBE                      tokenizer.getString(3), // type
                    // TODO DELETE MAYBE                      as,      // AllocSite pointer
                    // TODO DELETE MAYBE                      els,     // length IF applicable
                    // TODO DELETE MAYBE                      thread,  // thread Id
                    // TODO DELETE MAYBE                      Exec.NowUp() ); // Current time
                    AllocationTime = Heap.getAllocTime();
                    Exec.SetAllocTime( AllocationTime );
                    if (as) {
                        Exec.UpdateObj2AllocContext( obj,
                                                     as->getMethod()->getName() );
                    } else {
                        Exec.UpdateObj2AllocContext( obj,
                                                     "NOSITE" );
                    }
                    if (cckind == ExecMode::Full) {
                        // Get full stacktrace
                        DequeId_t strace = thread->stacktrace_using_id();
                        obj->setAllocContextList( strace );
                    }
                    total_objects++;
                }
                break;

            case 'U':
                {
                    // U <old-target> <object> <new-target> <field> <thread>
                    // 0      1          2         3           4        5
                    // -- Look up objects and perform update
                    unsigned int objId = tokenizer.getInt(2);
                    unsigned int tgtId = tokenizer.getInt(3);
                    unsigned int oldTgtId = tokenizer.getInt(1);
                    unsigned int threadId = tokenizer.getInt(5);
                    unsigned int field = tokenizer.getInt(4);
                    Thread *thread = Exec.getThread(threadId);
                    Object *oldObj = Heap.getObject(oldTgtId);
                    LastEvent lastevent = LastEvent::UPDATE_UNKNOWN;
                    Exec.IncUpdateTime();
                    obj = Heap.getObject(objId);
                    // NOTE that we don't need to check for non-NULL source object 'obj'
                    // here. NULL means that it's a global/static reference.
                    target = ((tgtId > 0) ? Heap.getObject(tgtId) : NULL);
                    if (obj) {
                        update_reference_summaries( obj, field, target );
                    }
                    // TODO last_map.setLast( threadId, LastEvent::UPDATE, obj );
                    // Set lastEvent and heap/stack flags for new target
                    if (target) {
                        if ( obj && 
                             obj != target &&
                             !(obj->wasRoot()) ) {
                            target->setPointedAtByHeap();
                        }
                        target->setLastTimestamp( Exec.NowUp() );
                        target->setActualLastTimestamp( Exec.NowUp() );
                        // TODO: Maybe LastUpdateFromStatic isn't the most descriptive
                        // So since target has an incoming edge, LastUpdateFromStatic
                        //    should be FALSE.
                        target->unsetLastUpdateFromStatic();
                    }
                    // Set lastEvent and heap/stack flags for old target
                    if (oldObj) {
                        // Set the last time stamp for Merlin Algorithm purposes
                        oldObj->setLastTimestamp( Exec.NowUp() );
                        oldObj->setActualLastTimestamp( Exec.NowUp() );
                        // Keep track of other properties
                        if (tgtId != 0) {
                            oldObj->unsetLastUpdateNull();
                        } else {
                            oldObj->setLastUpdateNull();
                        }
                        if (target) {
                            if (oldTgtId != tgtId) {
                                lastevent = LastEvent::UPDATE_AWAY_TO_VALID;
                                oldObj->setLastEvent( lastevent  );
                            }
                        } else {
                            // There's no need to check for oldTgtId == tgtId here.
                            lastevent = LastEvent::UPDATE_AWAY_TO_NULL;
                            oldObj->setLastEvent( lastevent );
                        }
                        if (field == 0) {
                            oldObj->setLastUpdateFromStatic();
                        } else {
                            oldObj->unsetLastUpdateFromStatic();
                        }
                    }
                    if (oldTgtId == tgtId) {
                        // It sometimes happens that the newtarget is the same as
                        // the old target. So we won't create any more new edges.
                        // DEBUG: cout << "UPDATE same new == old: " << target << endl;
                    } else {
                        if (obj) {
                            Edge *new_edge = NULL;
                            // Can't call updateField if obj is NULL
                            if (target) {
                                // Increment and decrement refcounts
                                unsigned int field_id = tokenizer.getInt(4);
                                new_edge = Heap.make_edge( obj, field_id,
                                                           target, Exec.NowUp() );
                            } 
                            Method *topMethod = NULL;
                            if (thread) {
                                topMethod = thread->TopMethod();
                            }
                            // TODO DELETE obj->updateField( new_edge,
                            // TODO DELETE                   field_id,
                            // TODO DELETE                   Exec.NowUp(),
                            // TODO DELETE                   topMethod, // for death site info
                            // TODO DELETE                   Reason::HEAP, // reason
                            // TODO DELETE                   NULL, // death root 0 because may not be a root
                            // TODO DELETE                   lastevent, // last event to determine cause
                            // TODO DELETE                   EdgeState::DEAD_BY_UPDATE, // edge is dead because of update
                            // TODO DELETE                   eifile ); // output edge info file
                            // NOTE: topMethod COULD be NULL here.
                            // DEBUG ONLY IF NEEDED
                            // Example:
                            // if ( (objId == tgtId) && (objId == 166454) ) {
                            // if ( (objId == 166454) ) {
                            //     tokenizer.debugCurrent();
                            // }
                        }
                    }
                }
                break;

            case 'D':
                {
                    // D <object> <thread-id>
                    // 0    1         2
                    unsigned int objId = tokenizer.getInt(1);
                    obj = Heap.getObject(objId);
                    if (obj) {
                        unsigned int now_uptime = Exec.NowUp();
                        // Merlin algorithm portion. TODO: This is getting unwieldy. Think about
                        // refactoring.
                        // Keep track of the latest death time
                        // TODO: Debug? Well it's a decent sanity check so we may leave it in.
                        assert( latest_death_time <= now_uptime );
                        // Ok, so now we can see if the death time has 
                        if (now_uptime > latest_death_time) {
                            // Do the Merlin algorithm
                            apply_merlin( new_garbage );
                            // TODO: What are the parameters?
                            //          - new_garbage for sure
                            //          - anything else?
                            // For now this simply updates the object death times
                        }
                        // Update latest death time
                        latest_death_time = now_uptime;

                        // The rest of the bookkeeping
                        new_garbage.push_back(obj);
                        unsigned int threadId = tokenizer.getInt(2);
                        LastEvent lastevent = obj->getLastEvent();
                        // Set the died by flags
                        if ( (lastevent == LastEvent::UPDATE_AWAY_TO_NULL) ||
                             (lastevent == LastEvent::UPDATE_AWAY_TO_VALID) ||
                             (lastevent == LastEvent::UPDATE_UNKNOWN) ) {
                            if (obj->wasLastUpdateFromStatic()) {
                                obj->setDiedByGlobalFlag();
                            }
                            // Design decision: all died by globals are
                            // also died by heap.
                            obj->setDiedByHeapFlag();
                        } else if ( (lastevent == LastEvent::ROOT) ||
                                    (lastevent == LastEvent::OBJECT_DEATH_AFTER_ROOT_DECRC) ||
                                    (lastevent == LastEvent::OBJECT_DEATH_AFTER_UPDATE_DECRC) ) {
                            obj->setDiedByStackFlag();
                        }
                        //else {
                        //     cerr << "Unhandled event: " << lastevent2str(lastevent) << endl;
                        // }
                        // TODO DELETE Heap.makeDead( obj,
                        // TODO DELETE                now_uptime,
                        // TODO DELETE                eifile );
                        // Get the current method
                        Method *topMethod = NULL;
                        // TODO: ContextPair cpair;
                        CPairType cptype;
                        Thread *thread;
                        if (threadId > 0) {
                            thread = Exec.getThread(threadId);
                            // Update counters in ExecState for map of
                            //   Object * to simple context pair
                        } else {
                            // No thread info. Get from ExecState
                            thread = Exec.get_last_thread();
                        }
                        if (thread) {
                            // TODO: cpair = thread->getContextPair();
                            // TODO: cptype = thread->getContextPairType();
                            topMethod = thread->TopMethod();
                            if (cckind == ExecMode::Full) {
                                // Get full stacktrace
                                DequeId_t strace = thread->stacktrace_using_id();
                                obj->setDeathContextList( strace );
                            }
                            // Set the death site
                            if (topMethod) {
                                obj->setDeathSite(topMethod);
                                Exec.UpdateObj2DeathContext( obj,
                                                             topMethod->getName() );
                            } else {
                                Exec.UpdateObj2DeathContext( obj,
                                                             "NULL_METHOD" );
                            }
                            // Death reason setting
                            Reason myreason;
                            if (thread->isLocalVariable(obj)) {
                                myreason = Reason::STACK;
                            } else {
                                myreason = Reason::HEAP;
                            }
                            // Recursively make the edges dead and assign the proper death cause
                            for ( EdgeMap::iterator p = obj->getEdgeMapBegin();
                                  p != obj->getEdgeMapEnd();
                                  ++p ) {
                                Edge* target_edge = p->second;
                                if (target_edge) {
                                    unsigned int fieldId = target_edge->getSourceField();
                                    // TODO DELETE obj->updateField( NULL,
                                    // TODO DELETE                   fieldId,
                                    // TODO DELETE                   Exec.NowUp(),
                                    // TODO DELETE                   topMethod,
                                    // TODO DELETE                   myreason,
                                    // TODO DELETE                   obj,
                                    // TODO DELETE                   lastevent,
                                    // TODO DELETE                   EdgeState::DEAD_BY_OBJECT_DEATH_NOT_SAVED,
                                    // TODO DELETE                   eifile ); // output edge info file
                                    // NOTE: STACK is used because the object that died,
                                    // died by STACK.
                                }
                            }
                        } // if (thread)
                        unsigned int rc = obj->getRefCount();
                        deathrc_map[objId] = rc;
                        not_candidate_map[objId] = (rc == 0);
                    } else {
                        assert(false);
                    } // if (obj) ... else
                }
                break;

            case 'M':
                {
                    // M <methodid> <receiver> <threadid>
                    // 0      1         2           3
                    // current_cc = current_cc->DemandCallee(method_id, object_id, thread_id);
                    // TEMP TODO ignore method events
                    method_id = tokenizer.getInt(1);
                    method = ClassInfo::TheMethods[method_id];
                    thread_id = tokenizer.getInt(3);
                    // Check to see if this is the main function
                    if (method == main_method) {
                        Exec.set_main_func_uptime( Exec.NowUp() );
                        Exec.set_main_func_alloctime( Exec.NowAlloc() );
                    }
                    Exec.Call(method, thread_id);
                }
                break;

            case 'E':
            case 'X':
                {
                    // E <methodid> <receiver> [<exceptionobj>] <threadid>
                    // 0      1         2             3             3/4
                    method_id = tokenizer.getInt(1);
                    method = ClassInfo::TheMethods[method_id];
                    thread_id = (tokenizer.numTokens() == 4) ? tokenizer.getInt(3)
                                                             : tokenizer.getInt(4);
                    Exec.Return(method, thread_id);
                }
                break;

            case 'T':
                // T <methodid> <receiver> <exceptionobj>
                // 0      1          2           3
                break;

            case 'H':
                // H <methodid> <receiver> <exceptionobj>
                break;

            case 'R':
                // R <objid> <threadid>
                // 0    1        2
                {
                    unsigned int objId = tokenizer.getInt(1);
                    Object *object = Heap.getObject(objId);
                    unsigned int threadId = tokenizer.getInt(2);
                    // cout << "objId: " << objId << "     threadId: " << threadId << endl;
                    if (object) {
                        object->setRootFlag(Exec.NowUp());
                        object->setLastEvent( LastEvent::ROOT );
                        object->setLastTimestamp( Exec.NowUp() );
                        object->setActualLastTimestamp( Exec.NowUp() );
                        Thread *thread = Exec.getThread(threadId);
                        if (thread) {
                            thread->objectRoot(object);
                        }
                    }
                    root_set.insert(objId);
                    // TODO last_map.setLast( threadId, LastEvent::ROOT, object );
                }
                break;

            default:
                // cout << "ERROR: Unknown entry " << tokenizer.curChar() << endl;
                break;
        }
    }
    cout << "DEBUG_STACK_EDGES: " << debug_stack_edges << endl;
    return total_objects;
}

// ----------------------------------------------------------------------
// Remove edges not in cyclic garbage
void filter_edgelist( deque< pair<int,int> >& edgelist, deque< deque<int> >& cycle_list )
{
    set<int> nodes;
    deque< pair<int,int> > newlist;
    for ( auto it = cycle_list.begin();
          it != cycle_list.end();
          ++it ) {
        for ( auto tmp = it->begin();
              tmp != it->end();
              ++tmp ) {
            nodes.insert(*tmp);
        }
    }
    for ( auto it = edgelist.begin();
          it != edgelist.end();
          ++it ) {
        auto first_it = nodes.find(it->first);
        if (first_it == nodes.end()) {
            // First node not found, carry on.
            continue;
        }
        auto second_it = nodes.find(it->second);
        if (second_it != nodes.end()) {
            // Found both edge nodes in cycle set 'nodes'
            // Add the edge.
            newlist.push_back(*it);
        }
    }
    edgelist = newlist;
}

unsigned int sumSize( std::set< Object * >& s )
{
    unsigned int total = 0;
    for ( auto it = s.begin();
          it != s.end();
          ++it ) {
        total += (*it)->getSize();
    }
    return total;
}

void update_summaries( Object *key,
                       std::set< Object * >& tgtSet,
                       GroupSum_t& pgs,
                       TypeTotalSum_t& tts,
                       SizeSum_t& ssum )
{
    string mytype = key->getType();
    unsigned gsize = tgtSet.size();
    // per group summary
    auto git = pgs.find(mytype);
    if (git == pgs.end()) {
        pgs[mytype] = std::move(std::vector< Summary * >());
    }
    unsigned int total_size = sumSize( tgtSet );
    Summary *s = new Summary( gsize, total_size, 1 );
    // -- third parameter is number of groups which is simply 1 here.
    pgs[mytype].push_back(s);
    // type total summary
    auto titer = tts.find(mytype);
    if (titer == tts.end()) {
        Summary *t = new Summary( gsize, total_size, 1 );
        tts[mytype] = t;
    } else {
        tts[mytype]->size += total_size;
        tts[mytype]->num_objects += tgtSet.size();
        tts[mytype]->num_groups++;
    }
    // size summary
    auto sit = ssum.find(gsize);
    if (sit == ssum.end()) {
        Summary *u = new Summary( gsize, total_size, 1 );
        ssum[gsize] = u;
    } else {
        ssum[gsize]->size += total_size;
        ssum[gsize]->num_groups++;
        // Doesn't make sense to make use of the num_objects field.
    }
}

void update_summary_from_keyset( KeySet_t &keyset,
                                 GroupSum_t &per_group_summary,
                                 TypeTotalSum_t &type_total_summary,
                                 SizeSum_t &size_summary )
{
    for ( auto it = keyset.begin();
          it != keyset.end();
          ++it ) {
        Object *key = it->first;
        std::set< Object * > *tgtSet = it->second;
        // TODO TODO 7 March 2016 - Put into CSV file.
        cout << "[ " << key->getType() << " ]: " << tgtSet->size() << endl;
        update_summaries( key,
                          *tgtSet,
                          per_group_summary,
                          type_total_summary,
                          size_summary );
    }
}


void summarize_reference_stability( EdgeSrc2Type_t &stability,
                                    EdgeSummary_t &my_refsum,
                                    Object2EdgeSrcMap_t &my_obj2ref )
{
    // Check each object to see if it's stable...(see ObjectRefType)
    for ( auto it = my_obj2ref.begin();
          it != my_obj2ref.end();
          ++it ) {
        // my_obj2ref : Object2EdgeSrcMap_t is a reverse map of sorts.
        //      For each object in the map, it maps to a vector of references
        //          that point/refer to that object
        // This Object is the target
        Object *obj = it->first;
        // This is the vector of references
        std::vector< EdgeSrc_t > reflist = it->second;
        // Convert to a set in order to remove possible duplicates
        std::set< EdgeSrc_t > refset( reflist.begin(), reflist.end() );
        if (!obj) {
            continue;
        }
        // obj is not NULL.
        // if ( (refset.size() <= 1) && (obj->wasLastUpdateNull() != true) ) {
        if (refset.size() <= 1) {
            obj->setRefTargetType( ObjectRefType::SINGLY_OWNED );
        } else {
            obj->setRefTargetType( ObjectRefType::MULTI_OWNED );
        }
    }
    // Check each edge source to see if its stable...(see EdgeSrcType)
    for ( auto it = my_refsum.begin();
          it != my_refsum.end();
          ++it ) {
        // my_refsum : EdgeSummary_t is a map from reference to a vector of
        //      objects that the reference pointed to. A reference is a pair
        //      of (Object pointer, fieldId).
        // Get the reference and deconstruct into parts.
        EdgeSrc_t ref = it->first;
        Object *obj = std::get<0>(ref); 
        FieldId_t fieldId = std::get<1>(ref); 
        // Get the vector/list of object pointers
        std::vector< Object * > objlist = it->second;
        // We need to make sure that all elements are not duplicates.
        std::set< Object * > objset( objlist.begin(), objlist.end() );
        // Is NULL in there?
        auto findit = objset.find(NULL);
        bool nullflag = (findit != objset.end());
        if (nullflag) {
            // If NULL is in the list, remove NULL.
            objset.erase(findit);
        }
        unsigned int size = objset.size();
        if (size == 1) {
            // The reference only points to one object!
            // Convert object set back into a vector.
            std::vector< Object * > tmplist( objset.begin(), objset.end() );
            assert( tmplist.size() == 1 );
            Object *tgt = tmplist[0];
            // Check for source and target death times
            // The checks for NULL are probably NOT necessary.
            assert(obj != NULL);
            VTime_t src_dtime = obj->getDeathTime();
            assert(tgt != NULL);
            VTime_t tgt_dtime = tgt->getDeathTime();
            if (tgt_dtime != src_dtime) {
                // UNSTABLE if target and source deathtimes are different
                // NOTE: This used to be UNSTABLE if target died before source.
                //       We are using a more conservative definition of stability here.
                stability[ref] = EdgeSrcType::UNSTABLE;
            } else {
                stability[ref] = (tgt->wasLastUpdateNull() ? EdgeSrcType::UNSTABLE : EdgeSrcType::STABLE);
            }
        } else {
            // objlist is of length > 1
            // This may still be stable if all objects in
            // the list are STABLE. Assume they're all stable unless
            // otherwise proven.
            stability[ref] = EdgeSrcType::SERIAL_STABLE; // aka serial monogamist
            for ( auto objit = objset.begin();
                  objit != objset.end();
                  ++objit ) {
                ObjectRefType objtype = (*objit)->getRefTargetType();
                if (objtype == ObjectRefType::MULTI_OWNED) {
                    stability[ref] = EdgeSrcType::UNSTABLE;
                    break;
                } else if (objtype == ObjectRefType::UNKNOWN) {
                    // Not sure if this is even possible.
                    // TODO: Make this an assertion?
                    // Let's assume that UNKNOWNs make it UNSTABLE.
                    stability[ref] = EdgeSrcType::UNSTABLE;
                    break;
                } // else continue
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ------[ OUTPUT FUNCTIONS ]-------------------------------------------------
// ---------------------------------------------------------------------------
void output_size_summary( string &dgroups_filename,
                          SizeSum_t &size_summary )
{
    ofstream dgroups_file(dgroups_filename);
    dgroups_file << "\"num_objects\",\"size_bytes\",\"num_groups\"" << endl;
    for ( auto it = size_summary.begin();
          it != size_summary.end();
          ++it ) {
        unsigned int gsize = it->first;
        Summary *s = it->second;
        dgroups_file << s->num_objects << ","
            << s->size << ","
            << s->num_groups << endl;
    }
    dgroups_file.close();
}

void output_type_summary( string &dgroups_by_type_filename,
                          TypeTotalSum_t &type_total_summary )
{
    ofstream dgroups_by_type_file(dgroups_by_type_filename);
    for ( TypeTotalSum_t::iterator it = type_total_summary.begin();
          it != type_total_summary.end();
          ++it ) {
        string myType = it->first;
        Summary *s = it->second;
        dgroups_by_type_file << myType << "," 
            << s->size << ","
            << s->num_groups  << ","
            << s->num_objects << endl;
    }
    dgroups_by_type_file.close();
}

// All sorts of hacky debug function. Very brittle.
void debug_type_algo( Object *object,
                      string& dgroup_kind )
{
    KeyType ktype = object->getKeyType();
    unsigned int objId = object->getId();
    if (ktype == KeyType::UNKNOWN_KEYTYPE) {
        cout << "ERROR: objId[ " << objId << " ] : "
             << "Keytype not set but algo determines [ " << dgroup_kind << " ]" << endl;
        return;
    }
    if (dgroup_kind == "CYCLE") {
        if (ktype != KeyType::CYCLE) {
            goto fail;
        }
    } else if (dgroup_kind == "CYCKEY") {
        if (ktype != KeyType::CYCLEKEY) {
            goto fail;
        }
    } else if (dgroup_kind == "DAG") {
        if (ktype != KeyType::DAG) {
            goto fail;
        }
    } else if (dgroup_kind == "DAGKEY") {
        if (ktype != KeyType::DAGKEY) {
            goto fail;
        }
    } else {
        cout << "ERROR: objId[ " << objId << " ] : "
             << "Unknown key type: " << dgroup_kind << endl;
    }
    return;
fail:
    cout << "ERROR: objId[ " << objId << " ] : "
         << "Keytype [ " << keytype2str(ktype) << " ]"
         << " doesn't match [ " << dgroup_kind << " ]" << endl;
    return;
}


void output_all_objects2( string &objectinfo_filename,
                          HeapState &myheap,
                          std::set<ObjectId_t> dag_keys,
                          std::set<ObjectId_t> dag_all_set,
                          std::set<ObjectId_t> all_keys,
                          unsigned int final_time )
{
    ofstream object_info_file(objectinfo_filename);
    object_info_file << "---------------[ OBJECT INFO ]--------------------------------------------------" << endl;
    const vector<string> header( { "objId", "createTime", "deathTime", "size", "type",
                                   "diedBy", "lastUpdate", "subCause", "clumpKind",
                                   "deathContext1", "deathContext2", "deathContextType",
                                   "allocContext1", "allocContext2", "allocContextType",
                                   "createTime_alloc", "deathTime_alloc",
                                   "allocSiteName", "stability", "last_actual_timestamp" } );

    for ( ObjectMap::iterator it = myheap.begin();
          it != myheap.end();
          ++it ) {
        Object *object = it->second;
        ObjectId_t objId = object->getId();
        KeyType ktype = object->getKeyType();
        string dgroup_kind;
        if (ktype == KeyType::CYCLE) {
            dgroup_kind = "CYC";
        } else if (ktype == KeyType::CYCLEKEY) {
            dgroup_kind = "CYCKEY";
        } else if (ktype == KeyType::DAG) {
            dgroup_kind = "DAG";
        } else if (ktype == KeyType::DAGKEY) {
            dgroup_kind = "DAGKEY";
        } else {
            dgroup_kind = "CYC";
        }
        string dtype;
        if (object->getDiedByStackFlag()) {
            dtype = "S"; // by stack
        } else if (object->getDiedByHeapFlag()) {
            if (object->wasLastUpdateFromStatic()) {
                dtype = "G"; // by static global
            } else {
                dtype = "H"; // by heap
            }
        } else {
            dtype = "E"; // program end
        }
        // TODO: Commented out the CONTEXT PAIR functionality that I'm not using.
        // TODO // Get the context pair and type for the allocation event
        // TODO ContextPair allocCpair = object->getAllocContextPair();
        // TODO Method *alloc_meth_ptr1 = std::get<0>(allocCpair);
        // TODO Method *alloc_meth_ptr2 = std::get<1>(allocCpair);
        // TODO string alloc_method1 = (alloc_meth_ptr1 ? alloc_meth_ptr1->getName() : "NONAME");
        // TODO string alloc_method2 = (alloc_meth_ptr2 ? alloc_meth_ptr2->getName() : "NONAME");
        // TODO // Get the context pair and type for the death event
        // TODO ContextPair deathCpair = object->getDeathContextPair();
        // TODO Method *death_meth_ptr1 = std::get<0>(deathCpair);
        // TODO Method *death_meth_ptr2 = std::get<1>(deathCpair);
        // TODO string death_method1 = (death_meth_ptr1 ? death_meth_ptr1->getName() : "NONAME");
        // TODO string death_method2 = (death_meth_ptr2 ? death_meth_ptr2->getName() : "NONAME");
        // END TODO: CONTEXT PAIR funcionality
        
        string death_method = object->getDeathContextSiteName();
        string allocsite_name = object->getAllocSiteName();
        ObjectRefType objstability = object->getRefTargetType();
        // S -> Stable
        // U -> Unstable
        // X -> Unknown
        string stability = ( (objstability == ObjectRefType::SINGLY_OWNED) ? "S"
                                   : (objstability == ObjectRefType::MULTI_OWNED ? "M" : "X") );
        unsigned int dtime = object->getDeathTime();
        object_info_file << objId
            << "," << object->getCreateTime()
            << "," << (dtime > 0 ? dtime : final_time)
            << "," << object->getSize()
            << "," << object->getType()
            << "," << dtype
            << "," << (object->wasLastUpdateNull() ? "NULL" : "VAL")
            << "," << (object->getDiedByStackFlag() ? (object->wasPointedAtByHeap() ? "SHEAP" : "SONLY")
                                                    : "H")
            << "," << dgroup_kind
            //--------------------------------------------------------------------------------
            << "," << death_method // Single level context for death
            // TODO << "," << death_method1 // Part 1 of simple context pair - death site
            //--------------------------------------------------------------------------------
            << "," << "X" //  padding - used to be death_method2
            // TODO << "," << death_method2 // part 2 of simple context pair - death site
            //--------------------------------------------------------------------------------
            << "," << "S" //  padding - used to be deathContextType - S for SINGLE
            // TODO << "," << (object->getDeathContextType() == CPairType::CP_Call ? "C" : "R") // C is call. R is return.
            //--------------------------------------------------------------------------------
            << "," << "X" //  padding - used to be alloc_method1
            // TODO << "," << alloc_method1 // Part 1 of simple context pair - alloc site
            //--------------------------------------------------------------------------------
            << "," << "X" //  padding - used to be alloc_method2
            // TODO << "," << alloc_method2 // part 2 of simple context pair - alloc site
            //--------------------------------------------------------------------------------
            << "," << "X" //  padding - used to be allocContextType
            // TODO << "," << (object->getAllocContextType() == CPairType::CP_Call ? "C" : "R") // C is call. R is return.
            //--------------------------------------------------------------------------------
            << "," << object->getCreateTimeAlloc()
            << "," << object->getDeathTimeAlloc()
            << "," << allocsite_name
            << "," << stability  // S, U, or X
            << "," << object->getActualLastTimestamp()
            << endl;
            // TODO: The following can be made into a lookup table:
            //       method names
            //       allocsite names
            //       type names
            // May only be necessary for performance reasons (ie, simulator eats up too much RAM 
            // on the larger benchmarks/programs.)
    }
    object_info_file << "---------------[ OBJECT INFO END ]----------------------------------------------" << endl;
    object_info_file.close();
}

void output_cycles( KeySet_t &keyset,
                    string &cycle_filename,
                    std::set<int> &node_set )
{
    ofstream cycle_file(cycle_filename);
    cycle_file << "---------------[ CYCLES ]-------------------------------------------------------" << endl;
    for ( KeySet_t::iterator it = keyset.begin();
          it != keyset.end();
          ++it ) {
        Object *obj = it->first;
        set< Object * > *sptr = it->second;
        unsigned int keyObjId = obj->getId();
        cycle_file << keyObjId;
        for ( set<Object *>::iterator tmp = sptr->begin();
              tmp != sptr->end();
              ++tmp ) {
            unsigned int tmpId = (*tmp)->getId();
            if (tmpId != keyObjId) {
                cycle_file  << "," << tmpId;
            }
            node_set.insert((*tmp)->getId());
        }
        cycle_file << endl;
    }
    cycle_file << "---------------[ CYCLES END ]---------------------------------------------------" << endl;
    cycle_file.close();
}

unsigned int output_edges( HeapState &myheap,
                           ofstream &edge_info_file )
{
    unsigned int total_done;
    // Iterate through 
    for ( auto it = myheap.begin_edgestate_map();
          it != myheap.end_edgestate_map();
          ++it ) {
        std::pair< Edge *, VTime_t > key = it->first;
        EdgeState estate = it->second;
        Edge *edge = std::get<0>(key);
        VTime_t ctime = std::get<1>(key);
        assert(edge);
        Object *src = edge->getSource();
        assert(src);
        VTime_t endtime = src->getDeathTime();
        if ( (estate == EdgeState::DEAD_BY_OBJECT_DEATH) ||
             (estate == EdgeState::DEAD_BY_UPDATE) ||
             (estate == EdgeState::DEAD_BY_PROGRAM_END) ) {
            // If the estate is any of these 3, then we've already
            // saved this edge. Log a WARNING.
            cerr << "WARNING: edge(" << edge->getSource()->getId() << ", "
                 << edge->getTarget()->getId() << ")[time: "
                 << edge->getCreateTime() << "] -> Estate["
                 << static_cast<int>(estate) << "]" << endl;
        } else {
            if (estate == EdgeState::DEAD_BY_OBJECT_DEATH_NOT_SAVED) {
                estate = EdgeState::DEAD_BY_OBJECT_DEATH;
            } else if (estate == EdgeState::DEAD_BY_PROGRAM_END_NOT_SAVED) {
                estate = EdgeState::DEAD_BY_PROGRAM_END;
            } else if ((estate == EdgeState::NONE) or
                       (estate == EdgeState::LIVE)) {
                cerr << "ERROR: edge(" << edge->getSource()->getId() << ", "
                     << edge->getTarget()->getId() << ")[time: "
                     << edge->getCreateTime() << "] -> Estate["
                     << static_cast<int>(estate) << "]" << endl;
                cerr << "Quitting." << endl;
                exit(100);
            } 
            output_edge( edge,
                         endtime,
                         estate,
                         edge_info_file );
            total_done++;
        }
    }
    return total_done;
}

#if 0
// Commented out code.
unsigned int output_edges_OLD( HeapState &myheap,
                               string &edgeinfo_filename )
{
    unsigned int total_edges = 0;
    ofstream edge_info_file(edgeinfo_filename);
    edge_info_file << "---------------[ EDGE INFO ]----------------------------------------------------" << endl;
    // srcId, tgtId, allocTime, deathTime
    for ( EdgeSet::iterator it = myheap.begin_edges();
          it != myheap.end_edges();
          ++it ) {
        Edge *eptr = *it;
        Object *source = eptr->getSource();
        Object *target = eptr->getTarget();
        assert(source);
        assert(target);
        unsigned int srcId = source->getId();
        unsigned int tgtId = target->getId();
        EdgeState estate = eptr->getEdgeState();
        unsigned int endtime = ( (estate == EdgeState::DEAD_BY_OBJECT_DEATH) ?
                                 source->getDeathTime() : eptr->getEndTime() );
        // TODO: This code was meant to filter out edges not belonging to cycles.
        //       But since we're also interested in the non-cycle nodes now, this is
        //       probably dead code and won't be used again. TODO
        // set<int>::iterator srcit = node_set.find(srcId);
        // set<int>::iterator tgtit = node_set.find(tgtId);
        // if ( (srcit != node_set.end()) || (srcit != node_set.end()) ) {
        // TODO: Header?
        edge_info_file << srcId << ","
            << tgtId << ","
            << eptr->getCreateTime() << ","
            << endtime << ","
            << eptr->getSourceField() << ","
            << static_cast<int>(estate) << endl;
        // }
        total_edges++;
    }
    edge_info_file << "---------------[ EDGE INFO END ]------------------------------------------------" << endl;
    edge_info_file.close();
    return total_edges;
}
#endif // if 0 

// ----------------------------------------------------------------------
// Output the map of death context site -> count of obects dying
void output_context_summary( string &context_death_count_filename,
                             ExecState &exstate )
{
    ofstream context_death_count_file(context_death_count_filename);
    for ( auto it = exstate.begin_deathCountmap();
          it != exstate.end_deathCountmap();
          ++it ) {
        string context = it->first;
        unsigned int total = it->second;
        context_death_count_file << context << "," 
                                 << total << endl;
    }
    context_death_count_file.close();
}

// ----------------------------------------------------------------------
// Output the map of simple context pair -> count of obects dying
// OLD CODE: Currently unused as we're not doing context pairs anymore
// TODO: void XXX_output_context_summary( string &context_death_count_filename,
// TODO:                                  ExecState &exstate )
// TODO: {
// TODO:     ofstream context_death_count_file(context_death_count_filename);
// TODO:     for ( auto it = exstate.begin_deathCountmap();
// TODO:           it != exstate.end_deathCountmap();
// TODO:           ++it ) {
// TODO:         ContextPair cpair = it->first;
// TODO:         Method *first = std::get<0>(cpair); 
// TODO:         Method *second = std::get<1>(cpair); 
// TODO:         unsigned int total = it->second;
// TODO:         unsigned int meth1_id = (first ? first->getId() : 0);
// TODO:         unsigned int meth2_id = (second ? second->getId() : 0);
// TODO:         string meth1_name = (first ? first->getName() : "NONAME");
// TODO:         string meth2_name = (second ? second->getName() : "NONAME");
// TODO:         char cptype = exstate.get_cptype_name(cpair);
// TODO:         context_death_count_file << meth1_name << "," 
// TODO:                                  << meth2_name << ","
// TODO:                                  << cptype << ","
// TODO:                                  << total << endl;
// TODO:     }
// TODO:     context_death_count_file.close();
// TODO: }

void output_reference_summary( string &reference_summary_filename,
                               string &ref_reverse_summary_filename,
                               string &stability_summary_filename,
                               EdgeSummary_t &my_refsum,
                               Object2EdgeSrcMap_t &my_obj2ref,
                               EdgeSrc2Type_t &stability )
{
    ofstream edge_summary_file(reference_summary_filename);
    ofstream reverse_summary_file(ref_reverse_summary_filename);
    ofstream stability_summary_file(stability_summary_filename);
    //
    // Summarizes the objects pointed at by the reference (object Id + field Id)
    // This is the REF-SUMMARY output file. In garbology, the ReferenceReader
    // is responsible for reading it.
    for ( auto it = my_refsum.begin();
          it != my_refsum.end();
          ++it ) {
        // Key is an edge source (which we used to call 'ref')
        EdgeSrc_t ref = it->first;
        Object *obj = std::get<0>(ref); 
        FieldId_t fieldId = std::get<1>(ref); 
        // Value is a vector of Object pointers
        std::vector< Object * > objlist = it->second;
        ObjectId_t objId = (obj ? obj->getId() : 0);
        edge_summary_file << objId << ","      // 1 - object Id
                          << fieldId << ",";   // 2 - field Id
        unsigned int actual_size = 0;
        for ( auto vecit = objlist.begin();
              vecit != objlist.end();
              ++vecit ) {
            if (*vecit) {
                ++actual_size;
            }
        }
        edge_summary_file << actual_size; // 3 - number of objects pointed at
        if (actual_size > 0) {
            for ( auto vecit = objlist.begin();
                  vecit != objlist.end();
                  ++vecit ) {
                if (*vecit) {
                    edge_summary_file << "," << (*vecit)->getId(); // 4+ - objects pointed at
                }
            }
        }
        edge_summary_file << endl;
    }
    //
    // Summarizes the reverse, which is for each object (using its Id), give
    // the references that point to it.
    // This is the REF-REVERSE-SUMMARY output file. In garbology, the ReverseRefReader
    // is responsible for reading it.
    for ( auto it = my_obj2ref.begin();
          it != my_obj2ref.end();
          ++it ) {
        Object *obj = it->first;
        std::vector< EdgeSrc_t > reflist = it->second;
        if (!obj) {
            continue;
        }
        // obj is not NULL.
        ObjectId_t objId = obj->getId();
        reverse_summary_file << objId << ","     // 1 - object Id
                             << reflist.size();  // 2 - number of references in lifetime
        for ( auto vecit = reflist.begin();
              vecit != reflist.end();
              ++vecit ) {
            Object *srcObj = std::get<0>(*vecit); 
            FieldId_t fieldId = std::get<1>(*vecit); 
            ObjectId_t srcId = (srcObj ? srcObj->getId() : 0);
            reverse_summary_file << ",(" << srcId << "," << fieldId << ")"; // 3+ - list of incoming references
        }
        reverse_summary_file << endl;
    }
    //
    // Summarize the stability attributes of features.
    //     output file is   *-STABILITY-SUMMARY.csv
    for ( auto it = stability.begin();
          it != stability.end();
          ++it ) {
        EdgeSrc_t ref = it->first;
        EdgeSrcType reftype = it->second;
        Object *obj = std::get<0>(ref); 
        FieldId_t fieldId = std::get<1>(ref); 
        ObjectId_t objId = (obj ? obj->getId() : 0);
        stability_summary_file << objId << ","            // 1 - object Id
                               << fieldId << ","          // 2 - field Id
                               << edgesrctype2shortstr(reftype)    // 3 - reference stability type
                               << endl;
    }
    // Close the files.
    edge_summary_file.close();
    reverse_summary_file.close();
    stability_summary_file.close();
}


// ----------------------------------------------------------------------

int main(int argc, char* argv[])
{
    if (argc != 7) {
        cout << "Usage: " << argv[0] << " <namesfile> <output base name>" << endl;
        cout << "      git version: " << build_git_sha << endl;
        cout << "      build date : " << build_git_time << endl;
        cout << "      CC kind    : " << Exec.get_kind() << endl;
        exit(1);
    }
    cout << "#     git version: " <<  build_git_sha << endl;
    cout << "#     build date : " <<  build_git_time << endl;
    cout << "---------------[ START ]-----------------------------------------------------------" << endl;
    string basename(argv[2]);
    // TODO
    string context_death_count_filename( basename + "-CONTEXT-DCOUNT.csv" );
    string call_context_filename( basename + "-CALL-CONTEXT.csv" );
    ofstream call_context_file(call_context_filename);
    Exec.set_output( &call_context_file );
    // TODO What is nodemap? TODO string nodemap_filename( basename + "-NODEMAP.csv" );
    // TODO What is nodemap? TODO ofstream nodemap_file(nodemap_filename);
    // TODO What is nodemap? TODO Exec.set_nodefile( &nodemap_file );

    // TODO Maybe keep. Change argv index if needed
    // TODO string main_class(argv[5]);
    // TODO string main_function(argv[6]);

    cout << "Read names file..." << endl;
    ClassInfo::read_names_file_no_mainfunc( argv[1] );

    cout << "Start trace..." << endl;
    FILE* f = fdopen(0, "r");
    unsigned int total_objects = read_trace_file(f);
    unsigned int final_time = Exec.NowUp() + 1;
    unsigned int final_time_alloc = Heap.getAllocTime();
    cout << "Done at update time: " << Exec.NowUp() << endl;
    cout << "Total objects: " << total_objects << endl;
    cout << "Heap.size:     " << Heap.size() << endl;
    // assert( total_objects == Heap.size() );
    // TODO MAYBE Heap.end_of_program( final_time );

    //---------------------------------------------------------------------
    string dsite_filename( basename + "-DSITES.csv" );
    ofstream dsite_file(dsite_filename);
    dsite_file << "---------------[ DEATH SITES INFO ]------------------------------------------------" << endl;
    for ( DeathSitesMap::iterator it = Heap.begin_dsites();
          it != Heap.end_dsites();
          ++it ) {
        Method *meth = it->first;
        set<string> *types = it->second;
        if (meth && types) {
            dsite_file << meth->getName() << "," << types->size();
            for ( set<string>::iterator sit = types->begin();
                  sit != types->end();
                  ++sit ) {
                dsite_file << "," << *sit;
            }
            dsite_file << endl;
        }
    }
    dsite_file << "---------------[ DEATH SITES INFO END ]--------------------------------------------" << endl;
    dsite_file.close();
    dsite_file << "---------------[ DONE ]------------------------------------------------------------" << endl;
    cout << "---------------[ DONE ]------------------------------------------------------------" << endl;
    cout << "#     git version: " <<  build_git_sha << endl;
    cout << "#     build date : " <<  build_git_time << endl;
}
