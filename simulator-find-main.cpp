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

using namespace std;

#include "tokenizer.h"
#include "classinfo.h"
#include "execution.h"
#include "heap.h"
#include "refstate.h"
#include "summary.hpp"
// #include "lastmap.h"
#include "version.hpp"

// ----------------------------------------------------------------------
// Types
class Object;
class CCNode;

// TODO typedef std::map< string, std::vector< Summary * > > GroupSum_t;

// ----------------------------------------------------------------------
//   Globals

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

// ----------------------------------------------------------------------
//   Analysis

// ----------------------------------------------------------------------
// TODO Delete?
bool member( Object* obj, const ObjectSet& theset )
{
    return theset.find(obj) != theset.end();
}

unsigned int closure( ObjectSet& roots,
                      ObjectSet& premarked,
                      ObjectSet& result )
{
    unsigned int mark_work = 0;

    vector<Object*> worklist;

    // -- Initialize the worklist with only unmarked roots
    for ( ObjectSet::iterator i = roots.begin();
          i != roots.end();
          i++ ) {
        Object* root = *i;
        if ( !member(root, premarked) ) {
            worklist.push_back(root);
        }
    }

    // -- Do DFS until the worklist is empty
    while (worklist.size() > 0) {
        Object* obj = worklist.back();
        worklist.pop_back();
        result.insert(obj);
        mark_work++;

        const EdgeMap& fields = obj->getFields();
        for ( EdgeMap::const_iterator p = fields.begin();
              p != fields.end();
              p++ ) {
            Edge* edge = p->second;
            Object* target = edge->getTarget();
            if (target) {
                // if (target->isLive(Exec.NowUp())) {
                if ( !member(target, premarked) &&
                     !member(target, result) ) {
                    worklist.push_back(target);
                }
                // } else {
                // cout << "WEIRD: Found a dead object " << target->info() << " from " << obj->info() << endl;
                // }
            }
        }
    }

    return mark_work;
}
// END TODO Delete?

unsigned int count_live( ObjectSet & objects, unsigned int at_time )
{
    int count = 0;
    // -- How many are actually live
    for ( ObjectSet::iterator p = objects.begin();
          p != objects.end();
          p++ ) {
        Object* obj = *p;
        if (obj->isLive(at_time)) {
            count++;
        }
    }

    return count;
}

// ----------------------------------------------------------------------
//   Read and process trace events

unsigned int read_trace_file(FILE* f)
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
    unsigned int total_objects;

    Method *main_method = ClassInfo::get_main_method();

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
                    unsigned int thrdid = (tokenizer.numTokens() == 6) ? tokenizer.getInt(5)
                                                                       : tokenizer.getInt(6);
                    Thread* thread = Exec.getThread(thrdid);
                    unsigned int els  = (tokenizer.numTokens() == 6) ? 0
                                                                     : tokenizer.getInt(5);
                    AllocSite* as = ClassInfo::TheAllocSites[tokenizer.getInt(4)];
                    assert(thread);
                    // Get context pair
                    ContextPair cpair = thread->getContextPair();
                    CPairType cptype = thread->getContextPairType();
                    // DEBUG
                    if (!as) {
                        cerr << "DBG: objId[ " << tokenizer.getInt(1) << " ] has no alloc site." << endl;
                    } // END DEBUG
                    obj = Heap.allocate( tokenizer.getInt(1),    // id
                                         tokenizer.getInt(2),    // size
                                         tokenizer.getChar(0),   // kind of alloc
                                         tokenizer.getString(3), // type
                                         as,      // AllocSite pointer
                                         els,     // length IF applicable
                                         thread,  // thread Id
                                         Exec.NowUp() ); // Current time
                    AllocationTime = Heap.getAllocTime();
                    Exec.SetAllocTime( AllocationTime );
                    Exec.UpdateObj2AllocContext( obj,
                                                 cpair,
                                                 cptype );
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
                        // TODO: Maybe LastUpdateFromStatic isn't the most descriptive
                        // So since target has an incoming edge, LastUpdateFromStatic
                        //    should be FALSE.
                        target->unsetLastUpdateFromStatic();
                    }
                    // Set lastEvent and heap/stack flags for old target
                    if (oldObj) {
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
                    } else if (obj && target) {
                        // Increment and decrement refcounts
                        unsigned int field_id = tokenizer.getInt(4);
                        Edge* new_edge = Heap.make_edge( obj, field_id,
                                                         target, Exec.NowUp() );
                        if (thread) {
                            Method *topMethod = thread->TopMethod();
                            if (topMethod) {
                                topMethod->getName();
                            }
                            obj->updateField( new_edge,
                                              field_id,
                                              Exec.NowUp(),
                                              topMethod, // for death site info
                                              HEAP, // reason
                                              NULL, // death root 0 because may not be a root
                                              lastevent ); // last event to determine cause
                            // NOTE: topMethod COULD be NULL here.
                        }
                        // DEBUG ONLY IF NEEDED
                        // Example:
                        // if ( (objId == tgtId) && (objId == 166454) ) {
                        // if ( (objId == 166454) ) {
                        //     tokenizer.debugCurrent();
                        // }
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
                        unsigned int threadId = tokenizer.getInt(2);
                        LastEvent lastevent = obj->getLastEvent();
                        // Set the died by flags
                        if ( (lastevent == UPDATE_AWAY_TO_NULL) ||
                             (lastevent == UPDATE_AWAY_TO_VALID) ||
                             (lastevent == UPDATE_UNKNOWN) ) {
                            if (obj->wasLastUpdateFromStatic()) {
                                obj->setDiedByGlobalFlag();
                            }
                            // Design decision: all died by globals are
                            // also died by heap.
                            obj->setDiedByHeapFlag();
                        } else if ( (lastevent == ROOT) ||
                                    (lastevent == OBJECT_DEATH_AFTER_ROOT_DECRC) ||
                                    (lastevent == OBJECT_DEATH_AFTER_UPDATE_DECRC) ) {
                            obj->setDiedByStackFlag();
                        } else {
                            cerr << "Unhandled event: " << lastevent2str(lastevent) << endl;
                        }
                        Heap.makeDead(obj, Exec.NowUp());
                        // Get the current method
                        Method *topMethod = NULL;
                        ContextPair cpair;
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
                            cpair = thread->getContextPair();
                            cptype = thread->getContextPairType();
                            Exec.UpdateObj2DeathContext( obj,
                                                         cpair,
                                                         cptype );
                            topMethod = thread->TopMethod();
                            if (cckind == ExecMode::Full) {
                                // Get full stacktrace
                                DequeId_t strace = thread->stacktrace_using_id();
                                obj->setDeathContextList( strace );
                            }
                            // Set the death site
                            if (topMethod) {
                                obj->setDeathSite(topMethod);
                            } 
                            Reason myreason;
                            if (thread->isLocalVariable(obj)) {
                                myreason = STACK;
                            } else {
                                myreason = HEAP;
                            }
                            // Recursively make the edges dead and assign the proper death cause
                            for ( EdgeMap::iterator p = obj->getEdgeMapBegin();
                                  p != obj->getEdgeMapEnd();
                                  ++p ) {
                                Edge* target_edge = p->second;
                                if (target_edge) {
                                    unsigned int fieldId = target_edge->getSourceField();
                                    obj->updateField( NULL,
                                                      fieldId,
                                                      Exec.NowUp(),
                                                      topMethod,
                                                      myreason,
                                                      obj,
                                                      lastevent );
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


// ---------------------------------------------------------------------------
// ------[ OUTPUT FUNCTIONS ]-------------------------------------------------
// ---------------------------------------------------------------------------
// ----------------------------------------------------------------------
int main(int argc, char* argv[])
{
    if (argc != 3) {
        cout << "Usage: " << argv[0] << " <namesfile> <main.package> <output filename>" << endl;
        cout << "      git version: " << build_git_sha << endl;
        cout << "      build date : " << build_git_time << endl;
        cout << "      CC kind    : " << Exec.get_kind() << endl;
        exit(1);
    }
    cout << "#     git version: " <<  build_git_sha << endl;
    cout << "#     build date : " <<  build_git_time << endl;
    cout << "---------------[ START ]-----------------------------------------------------------" << endl;
    string basename(argv[3]);
    // TODO TODO string summary_filename( basename + "-SUMMARY.csv" );
    Exec.set_output( NULL );

    string main_package(argv[2]);

    // TODO Need the main package name
    cout << "Read names file..." << endl;
    ClassInfo::read_names_file( argv[1], 
                                main_package );

    cout << "Start trace..." << endl;
    FILE* f = fdopen(0, "r");
    unsigned int total_objects = read_trace_file(f);
    unsigned int final_time = Exec.NowUp();
    unsigned int final_time_alloc = Heap.getAllocTime();
    cout << "Done at update time: " << Exec.NowUp() << endl;
    cout << "Total objects: " << total_objects << endl;
    cout << "Heap.size:     " << Heap.size() << endl;
    // assert( total_objects == Heap.size() );
    Heap.end_of_program(Exec.NowUp() + 1);

    if (cycle_flag) {
        std::deque< pair<int,int> > edgelist; // TODO Do we need the edgelist?
        // per_group_summary: type -> vector of group summary
        GroupSum_t per_group_summary;
        // type_total_summary: summarize the stats per type
        TypeTotalSum_t type_total_summary;
        // size_summary: per group size summary. That is, for each group of size X,
        //               add up the sizes.
        SizeSum_t size_summary;
        // Remember the key objects for non-cyclic death groups.
        set<ObjectId_t> dag_keys;
        deque<ObjectId_t> dag_all;
        // Reference stability summary
        Ref2Type_t stability_summary;
        // Lambdas for utility
        auto lfn = [](Object *ptr) -> unsigned int { return ((ptr) ? ptr->getId() : 0); };
        auto ifNull = [](Object *ptr) -> bool { return (ptr == NULL); };

        for ( KeySet_t::iterator kiter = keyset.begin();
              kiter != keyset.end();
              kiter++ ) {
            Object *optr = kiter->first;
            ObjectId_t objId = (optr ? optr->getId() : 0); 
            dag_keys.insert(objId);
            dag_all.push_back(objId);
            set< Object * > *sptr = kiter->second;
            if (!sptr) {
                continue; // TODO
            }
            deque< Object * > deqtmp;
            // std::copy( sptr->begin(), sptr->end(), deqtmp.begin() );
            // std::remove_if( deqtmp.begin(), deqtmp.end(), ifNull );
            for ( set< Object * >::iterator setit = sptr->begin();
                  setit != sptr->end();
                  setit++ ) {
                if (*setit) {
                    deqtmp.push_back( *setit );
                }
            }
            if (deqtmp.size() > 0) {
                // TODO Not sure why this transform isn't working like the for loop.
                // Not too important, but kind of curious as to how I'm not using
                // std::transform properly.
                // TODO
                // std::transform( deqtmp.cbegin(),
                //                 deqtmp.cend(),
                //                 dag_all.begin(),
                //                 lfn );
                for ( deque< Object * >::iterator dqit = deqtmp.begin();
                      dqit != deqtmp.end();
                      dqit++ ) {
                    if (*dqit) {
                        dag_all.push_back( (*dqit)->getId() );
                    }
                }
            }
        }
        // Copy all dag_all object Ids into dag_all_set to get rid of duplicates.
        set<ObjectId_t> dag_all_set( dag_all.cbegin(), dag_all.cend() );

        // scan_queue2 determines all the death groups that are cyclic
        // The '2' is a historical version of the function that won't be
        // removed.
        Heap.scan_queue2( edgelist,
                          not_candidate_map );
        update_summary_from_keyset( keyset,
                                    per_group_summary,
                                    type_total_summary,
                                    size_summary );
        // Save key object IDs for _all_ death groups.
        set<ObjectId_t> all_keys;
        for ( KeySet_t::iterator kiter = keyset.begin();
              kiter != keyset.end();
              kiter++ ) {
            Object *optr = kiter->first;
            ObjectId_t objId = (optr ? optr->getId() : 0); 
            all_keys.insert(objId);
            // NOTE: We don't really need to add ALL objects here since
            // we can simply test against dag_all_set to see if it's a DAG
            // object. If not in dag_all_set, then it's a CYC object.
        }

        // Analyze the edge summaries
        summarize_reference_stability( stability_summary,
                                       ref_summary,
                                       obj2ref_map );
        // ----------------------------------------------------------------------
        // OUTPUT THE SUMMARIES
        // By size summary of death groups
        output_size_summary( dgroups_filename,
                             size_summary );
        // Type total summary output
        output_type_summary( dgroups_by_type_filename,
                             type_total_summary );
        // Output all objects info
        output_all_objects2( objectinfo_filename,
                             Heap,
                             dag_keys,
                             dag_all_set,
                             all_keys );
        output_context_summary( context_death_count_filename,
                                Exec );
        output_reference_summary( reference_summary_filename,
                                  ref_reverse_summary_filename,
                                  stability_summary_filename,
                                  ref_summary,
                                  obj2ref_map,
                                  stability_summary );
        // TODO: What next? 
        // Output cycles
        set<int> node_set;
        output_cycles( keyset,
                       cycle_filename,
                       node_set );
        // Output all edges
        unsigned int total_edges = output_edges( Heap,
                                                 edgeinfo_filename );
    } else {
        cout << "NOCYCLE chosen. Skipping cycle detection." << endl;
    }

    ofstream summary_file(summary_filename);
    summary_file << "---------------[ SUMMARY INFO ]----------------------------------------------------" << endl;
    summary_file << "number_of_objects," << Heap.size() << endl
                 << "number_of_edges," << Heap.numberEdges() << endl
                 << "died_by_stack," << Heap.getTotalDiedByStack2() << endl
                 << "died_by_heap," << Heap.getTotalDiedByHeap2() << endl
                 << "died_by_global," << Heap.getTotalDiedByGlobal() << endl
                 << "died_at_end," << Heap.getTotalDiedAtEnd() << endl
                 << "last_update_null," << Heap.getTotalLastUpdateNull() << endl
                 << "last_update_null_heap," << Heap.getTotalLastUpdateNullHeap() << endl
                 << "last_update_null_stack," << Heap.getTotalLastUpdateNullStack() << endl
                 << "last_update_null_size," << Heap.getSizeLastUpdateNull() << endl
                 << "last_update_null_heap_size," << Heap.getSizeLastUpdateNullHeap() << endl
                 << "last_update_null_stack_size," << Heap.getSizeLastUpdateNullStack() << endl
                 << "died_by_stack_only," << Heap.getDiedByStackOnly() << endl
                 << "died_by_stack_after_heap," << Heap.getDiedByStackAfterHeap() << endl
                 << "died_by_stack_only_size," << Heap.getSizeDiedByStackOnly() << endl
                 << "died_by_stack_after_heap_size," << Heap.getSizeDiedByStackAfterHeap() << endl
                 << "no_death_sites," << Heap.getNumberNoDeathSites() << endl
                 << "size_died_by_stack," << Heap.getSizeDiedByStack() << endl
                 << "size_died_by_heap," << Heap.getSizeDiedByHeap() << endl
                 << "size_died_at_end," << Heap.getSizeDiedAtEnd() << endl
                 << "vm_RC_zero," << Heap.getVMObjectsRefCountZero() << endl
                 << "vm_RC_positive," << Heap.getVMObjectsRefCountPositive() << endl
                 << "max_live_size," << Heap.maxLiveSize() << endl
                 << "final_time," << final_time << endl
                 << "final_time_alloc," << final_time_alloc << endl;
    summary_file << "---------------[ SUMMARY INFO END ]------------------------------------------------" << endl;
    summary_file.close();
    //---------------------------------------------------------------------
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
