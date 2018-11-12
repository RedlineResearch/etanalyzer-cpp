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
#include "version.hpp"

#include "memorymgr.h"

// ----------------------------------------------------------------------
// Types
class Object;
class CCNode;

typedef std::map< string, std::vector< Summary * > > GroupSum_t;
typedef std::map< string, Summary * > TypeTotalSum_t;
typedef std::map< unsigned int, Summary * > SizeSum_t;

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

// ----------------------------------------------------------------------
//   Analysis
set<unsigned int> root_set;


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
    unsigned int total_objects = 0;

    // -- Allocation time
    unsigned int AllocationTime = 0;
    while ( !tokenizer.isDone() ) {
        tokenizer.getLine();
        if (tokenizer.isDone()) {
            break;
        }
        if (Exec.NowUp() % 10000000 == 1) {
            cout << "  Method time: " << Exec.NowUp() << "   Alloc time: " << AllocationTime << endl;
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
                    AllocSite* as = ClassInfo::TheAllocSites[tokenizer.getInt(4)];
                    obj = Heap.allocate( tokenizer.getInt(1),
                                         tokenizer.getInt(2),
                                         tokenizer.getChar(0),
                                         tokenizer.getString(3),
                                         as,
                                         els,
                                         thread,
                                         Exec.NowUp() );
                    unsigned int old_alloc_time = AllocationTime;
                    AllocationTime += obj->getSize();
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
                    if ((objId > 0) && (tgtId > 0)) {
                        Heap.make_edge2( objId, tgtId );
                    }
                    if ((objId > 0) && (oldTgtId > 0)) {
                        Heap.remove_edge2( objId, oldTgtId );
                    }
                    // TODO ============================================================ TODO
                    // TODO unsigned int threadId = tokenizer.getInt(5);
                    // TODO: Not sure if we need this code
                    // TODO Thread *thread = Exec.getThread(threadId);
                    // TODO Object *oldObj = Heap.getObject(oldTgtId);
                    // TODO obj = Heap.getObject(objId);
                    // TODO target = ((tgtId > 0) ? Heap.getObject(tgtId) : NULL);
                    // TODO if (obj && target) {
                        // TODO unsigned int field_id = tokenizer.getInt(4);
                        // TODO Edge* new_edge = Heap.make_edge( obj, field_id,
                        // TODO                                  target, Exec.NowUp() );
                        // TODO if (thread) {
                        // TODO     Method *topMethod = thread->TopMethod();
                        // TODO     if (topMethod) {
                        // TODO         topMethod->getName();
                        // TODO     }
                        // TODO     obj->updateField( new_edge,
                        // TODO                       field_id,
                        // TODO                       Exec.NowUp(),
                        // TODO                       topMethod, // for death site info
                        // TODO                       HEAP, // reason
                        // TODO                       NULL ); // death root 0 because may not be a root
                        // TODO     // NOTE: topMethod COULD be NULL here.
                        // TODO }
                        // DEBUG ONLY IF NEEDED
                        // Example:
                        // if ( (objId == tgtId) && (objId == 166454) ) {
                        // if ( (objId == 166454) ) {
                        //     tokenizer.debugCurrent();
                        // }
                    // TODO }
                    // TODO ============================================================ TODO
                }
                break;

            case 'D':
                {
                    // D <object> <thread-id>
                    // 0    1
                    unsigned int objId = tokenizer.getInt(1);
                    obj = Heap.getObject(objId);
                    if (obj) {
                        unsigned int threadId = tokenizer.getInt(2);
                        Thread *thread = Exec.getThread(threadId);
                        Heap.makeDead(obj, Exec.NowUp());
                    } else {
                        assert(false);
                    }
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
    return total_objects;
}

void debug_GC_history( deque< GCRecord_t > &GC_history )
{
    for ( deque< GCRecord_t >::iterator iter = GC_history.begin();
          iter != GC_history.end();
          ++iter ) {
        cout << "[" << iter->first << "] - " << iter->second << " bytes" << endl;
    }
}

// ----------------------------------------------------------------------

int main(int argc, char* argv[])
{
    if (argc != 5) {
        cout << "simulator-GC VERSION 2" << endl
             << "Usage: " << argv[0] << " <namesfile> <dgroups-csv-file> <output base name> <memsize>" << endl
             << "      git version: " <<  build_git_sha << endl
             << "      build date : " <<  build_git_time << endl;
        exit(1);
    }
    string dgroups_csvfile(argv[2]);
    string basename(argv[3]);

    unsigned long memsize = std::stoul(argv[4]);
    cout << "Memory size: " << memsize << " bytes." << endl;
    
    Heap.initialize_memory_deferred_VER2( memsize, // in bytes
                                          dgroups_csvfile, // output of dgroups2db.py
                                          3 ); // Number of groups to use

    // Hard coded number at this point. TODO
    cout << "Read names file..." << endl;
    ClassInfo::read_names_file_no_mainfunc( argv[1] );

    cout << "Start trace..." << endl;
    FILE* f = fdopen(0, "r");
    unsigned int total_objects = read_trace_file(f);
    unsigned int final_time = Exec.NowUp();
    cout << "Done at time " << Exec.NowUp() << endl
         << "Total objects: " << total_objects << endl
         << "Total allocated in bytes:     " << Heap.get_total_alloc() << endl
         << "Number of collections: " << Heap.get_number_of_collections() << endl;
    cout << "Mark total   : " << Heap.get_mark_total() << endl
         << "- mark saved : " << Heap.get_mark_saved() << endl
         << "- total alloc: " << Heap.get_total_alloc() << endl;
    // TODO << " - number of edges removed: " << Heap.get_number_edges_removed() << endl
    // TODO << " - number of edges removal attempts: " << Heap.get_number_attempts_edges_removed() << endl
    // TODO << " - number of region edges: " << Heap.get_region_edges_count() << endl
    // TODO << " - number of in edges: " << Heap.get_in_edges_count() << endl
    // TODO << " - number of out edges: " << Heap.get_out_edges_count() << endl
    // TODO << " - number of nonregion edges: " << Heap.get_nonregion_edges_count() << endl;
    return Heap.get_number_of_collections();
}

//--------------------------------------------------------------------------------
// COMMENTED OUT CODE
//--------------------------------------------------------------------------------
/*

void sanity_check()
{
   if (Now > obj->getDeathTime() && obj->getRefCount() != 0) {
   nonzero_ref++;
   printf(" Non-zero-ref-dead %X of type %s\n", obj->getId(), obj->getType().c_str());
   }
}

bool member( Object* obj, const ObjectSet& theset )
{
    return theset.find(obj) != theset.end();
}

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

*/
