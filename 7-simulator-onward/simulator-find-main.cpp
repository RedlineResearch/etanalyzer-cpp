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

// -- Keyset needed for Heap but isn't really needed.
KeySet_t keyset;
// -- Object to key object map. Also NOT needed.
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
    unsigned int total_objects = 0;

    Method *main_method = ClassInfo::get_main_method();
    unsigned int main_id = main_method->getId();

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

        // DEBUG only
        // if (Exec.NowUp() % 105000 == 100000) {
        //     // cout << "  Method time: " << Exec.Now() << "   Alloc time: " << AllocationTime << endl;
        //     cout << "  Update time: " << Exec.NowUp() << " | Method time: TODO | Alloc time: " << AllocationTime << endl;
        // }

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
                                                                     : ;
                    AllocSite* as = ClassInfo::TheAllocSites[tokenizer.getInt(4)];
                    string sitename("DONTCARE");
                    assert(thread);
                    // We need to keep track of allocation time.
                    obj = Heap.allocate( tokenizer.getInt(1),    // id
                                         tokenizer.getInt(2),    // size
                                         tokenizer.getChar(0),   // kind of alloc
                                         tokenizer.getString(3), // type
                                         as,      // AllocSite pointer
                                         sitename, // Non Java lib allocsite name
                                         els,     // length IF applicable
                                         thread,  // thread Id
                                         Exec.NowUp() ); // Current time
                    AllocationTime = Heap.getAllocTime();
                    Exec.SetAllocTime( AllocationTime );
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
                    // TODO TODO Object *oldObj = Heap.getObject(oldTgtId);
                    // NOTE: We don't really need the edges here.
                    Exec.IncUpdateTime();
                }
                break;

            case 'D':
                // D <object> <thread-id>
                // 0    1         2
                // We really don't need anything here.
                break;

            case 'M':
                {
                    // M <methodid> <receiver> <threadid>
                    // 0      1         2           3
                    // current_cc = current_cc->DemandCallee(method_id, object_id, thread_id);
                    // TEMP TODO ignore method events
                    method_id = tokenizer.getInt(1);
                    method = ClassInfo::TheMethods[method_id];
                    // check to see if this is THE main method.
                    if (method == main_method) {
                        // We're done!
                        cout << "main_time:" << Exec.NowUp() << endl;
                        cout << "alloc_time:" << Exec.NowAlloc() << endl;
                        return Exec.NowUp();
                    }
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
                // We really don't need anything here.
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
    if (argc != 5) {
        cout << "Usage: " << argv[0] << " <namesfile> <main.class> <main.function> <output filename>" << endl;
        cout << "      git version: " << build_git_sha << endl;
        cout << "      build date : " << build_git_time << endl;
        cout << "      CC kind    : " << Exec.get_kind() << endl;
        exit(1);
    }
    cout << "#     git version: " <<  build_git_sha << endl;
    cout << "#     build date : " <<  build_git_time << endl;
    cout << "---------------[ START ]-----------------------------------------------------------" << endl;
    string basename(argv[4]);
    // TODO TODO string summary_filename( basename + "-SUMMARY.csv" );
    Exec.set_output( NULL );

    string main_class(argv[2]);
    string main_function(argv[3]);

    // TODO Need the main package name
    cout << "Read names file..." << endl;
    ClassInfo::read_names_file( argv[1], 
                                main_class,
                                main_function );

    cout << "Start trace..." << endl;
    FILE* f = fdopen(0, "r");
    unsigned int main_time = read_trace_file(f);

    cout << "---------------[ DONE ]------------------------------------------------------------" << endl;
    cout << "#     git version: " <<  build_git_sha << endl;
    cout << "#     build date : " <<  build_git_time << endl;
}
