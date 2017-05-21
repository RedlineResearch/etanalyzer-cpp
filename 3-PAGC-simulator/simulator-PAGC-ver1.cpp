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

#include <boost/circular_buffer.hpp>

using namespace std;

#include "tokenizer.h"
#include "classinfo.h"
#include "execution.h"
#include "heap.h"
#include "func_record.h"
#include "summary.hpp"
#include "version.hpp"

#include "memorymgr.h"

// ----------------------------------------------------------------------
// Types
class Object;
class CCNode;

// BRAINSTORM 1:
// - Each node in the map is the static function.
// - The value is the garbage for all instances of that function.
// - NEED: a sense of how often the function actually produces garbage:
//       + a list of garbage instances including 0
//       + mean, stddev, etc to indication dispersion
// - In addition to the garbage value, we have a list of all
//      sub functions called from this function, AND
//      the garbage of the sub function from that function only
//      PROBLEM: What of any sub functions called further down the chain?
//      QUESTION: What does this buy us?
// - Possible SOLUTION: For every calling context up to a function of interest F,
//                      Remember the possible call chain.
//                      OR, for every function, remember the possible calling function.
//                      as in:
//                          tgt_func -> [ calling_func list ]
// Design 1:
//  MAP:
//      func -> record {
//         vector of garbage amount at each exit
//         map:
//            func -> total amount
//      }
typedef struct {
    public:
        void add_garbage( unsigned int garbage )
        {
            this->garbage_vector.push_back( garbage );
            // TODO: Maybe pass a std::vector or std::map of sub functions
            //       and associated garbage from those sub calls.
        };
    private:
        std::vector< unsigned int > garbage_vector;
        std::map< MethodId_t, unsigned int > subfunc_map;
} FunctionRec_t;

typedef std::map< MethodId_t, FunctionRec_t > FunctionRec_map_t;

typedef std::map< string, std::vector< Summary * > > GroupSum_t;
typedef std::map< string, Summary * > TypeTotalSum_t;
typedef std::map< unsigned int, Summary * > SizeSum_t;

typedef std::map< unsigned int, unsigned int > ObjectMap_t;
// Map from object ID to object size
// ----------------------------------------------------------------------
//   Globals

// -- The pseudo-heap
ObjectMap_t objmap;

// TODO: DELETE HeapState Heap( whereis, keyset );

// -- Execution state
#ifdef ENABLE_TYPE1
ExecMode cckind = ExecMode::Full; // Full calling context
#else
ExecMode cckind = ExecMode::StackOnly; // Stack-only context
#endif // ENABLE_TYPE1

ExecState Exec(cckind);

FunctionRec_map_t fnrec_map;

// -- Turn on debugging
bool debug = false;

// ----------------------------------------------------------------------
//   Analysis
set<unsigned int> root_set;


// TODO:
// 1. Keep track of lifetime garbage.
//      * On D(eath) event, increase garabage.
//      ? Can we figure out where it died? Probably not yet.
// 2. Keep track of heuristic garbage.
//      - this depends on heuristic function?
// 3. Keep track of other garbage characteristics:
//      - per function garbage maybe?
//      - minimum, maximum per function exit
// 4. Create the heuristic function.
// 5. Where does this function get called from?
//      -> On the function exits of certain functions.
// 6. Therefore we need to read in the list of functions we want to do this on.
// ----------------------------------------------------------------------
//   Read and process trace events

unsigned int read_trace_file( FILE *f,
                              ofstream &dataout )
// TODO: CHOOSE A DESIGN:
//  Option 1: We are simply processing the file here for further analysis later.
//            So we don't need to choose the functions here.
//            + This will most likely save time in processing as we don't
//              repeatedly need all object allocations.
//            - Adds to development time because we need to do the post-processing
//              analysis.
//  Option 2: We do the processing here and we have the functions that we are
//            interested in.
//            + No need for post analysis.
//            - Needs a run script to go through the functions
//            - A lot of repeated work in reading in events we don't need.
//              => This repeated work may be well worth the possible additional dev time.
//
//  Save the following events as processed from the original ET trace:
//
//  F <func name ID> cumulative total exits so far
//  G <garbage cumulative size>
//  A <allocation time == allocation cumulative size>
//
//  On the analysis, we only take a relatively small subset to try to do a regression analysis
//  (or some other predictive analysis).
//  EXPERIMENT 1: We choose the functions a priori.
//  EXPERIMENT 2: We search for the best functions. We limit the number of functions.
//     Sub EXPermient 2b: We could selectively go determine how many functions might help.
{
    Tokenizer tokenizer(f);

    unsigned int method_id;
    unsigned int object_id;
    unsigned int target_id;
    unsigned int field_id;
    unsigned int thread_id;
    unsigned int exception_id;
    // Object *obj;
    // Object *target;
    Method *method;
    unsigned int total_garbage = 0;

    // A map from thread ID to garbage amount for the current function
    std::map< unsigned int, std::vector< unsigned int > > tid2gstack;
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
                    unsigned int thrdid = (tokenizer.numTokens() == 6) ? tokenizer.getInt(5)
                                                                       : tokenizer.getInt(6);
                    Thread* thread = Exec.getThread(thrdid);
                    unsigned int els  = (tokenizer.numTokens() == 6) ? 0
                                                                     : tokenizer.getInt(5);
                    AllocSite* as = ClassInfo::TheAllocSites[tokenizer.getInt(4)];
                    unsigned int my_id = tokenizer.getInt(1);
                    unsigned int my_size = tokenizer.getInt(2);
                    objmap[my_id] = my_size;
                    // obj = Heap.allocate( tokenizer.getInt(1),
                    //                      tokenizer.getInt(2),
                    //                      tokenizer.getChar(0),
                    //                      tokenizer.getString(3),
                    //                      as,
                    //                      els,
                    //                      thread,
                    //                      Exec.NowUp() );
                    unsigned int old_alloc_time = AllocationTime;
                    AllocationTime += my_size;
                    dataout << "A," << AllocationTime << endl;
                }
                break;

            case 'U':
                {
                    // U <old-target> <object> <new-target> <field> <thread>
                    // 0      1          2         3           4        5
                    // -- Look up objects and perform update
                    // TODO: unsigned int objId = tokenizer.getInt(2);
                    // TODO: unsigned int tgtId = tokenizer.getInt(3);
                    // TODO: unsigned int oldTgtId = tokenizer.getInt(1);
                }
                break;

            case 'D':
                {
                    // D <object> <thread-id>
                    // 0    1
                    object_id = tokenizer.getInt(1);
                    // 1. Get the thread
                    thread_id = tokenizer.getInt(2);
                    unsigned int my_size = objmap[object_id];
                    total_garbage += my_size;
                    // 2. Save in accumulator. The sum will be saved in 
                    //    fnrec_map when the function exits.
                    auto iter = tid2gstack.find(thread_id);
                    if (iter != tid2gstack.end()) {
                        tid2gstack[thread_id].back() += my_size;
                    } else {
                        std::vector< unsigned int > tmp(1, my_size);
                        tid2gstack[thread_id] = tmp;
                    }
                    dataout << "G," << total_garbage << endl;
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
                    // Save in garbage stack
                    auto iter = tid2gstack.find(thread_id);
                    if (iter == tid2gstack.end()) {
                        std::vector< unsigned int > tmp;
                        tid2gstack[thread_id] = tmp;
                    }
                    tid2gstack[thread_id].push_back(0);
                    // TODO: dataout << "F," << method_id << endl;
                    // PROBLEM: How do we assign garbage deaths to methods efficiently?
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
                    dataout << "E," << method_id << endl;
                    // Pop off the garbage stack and save in map
                    auto iter = tid2gstack.find(thread_id);
                    if (iter != tid2gstack.end()) {
                        unsigned int stack_garbage = tid2gstack[thread_id].back();
                        tid2gstack[thread_id].pop_back();
                        auto iter = fnrec_map.find(method_id);
                        if (iter == fnrec_map.end()) {
                            FunctionRec_t tmp;
                            fnrec_map[method_id] = tmp;
                        }
                        fnrec_map[method_id].add_garbage( stack_garbage );
                    } else {
                        cerr << "Method EXIT: Empty garbage stack for thread id"
                             << thread_id << "." << endl;
                        //    the else clause shouldn't be possible, but it's worth
                        //    investigating if this happens.
                        //    TODO TODO TODO
                        //    Add some more debugging code if this happens.
                    }

                    tid2gstack[thread_id].push_back(0);
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
                    // TODO: unsigned int objId = tokenizer.getInt(1);
                    // TODO: Do we need to do something here?
                }
                break;

            default:
                // cout << "ERROR: Unknown entry " << tokenizer.curChar() << endl;
                break;
        }
    }
    return total_garbage;
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
    if (argc != 3) {
        cout << "simulator-PAGC-ver1" << endl
             << "Usage: " << argv[0] << " <namesfile>  <output base name>" << endl
             << "      git version: " <<  build_git_sha << endl
             << "      build date : " <<  build_git_time << endl;
        exit(1);
    }
    cout << "Read names file..." << endl;
    ClassInfo::read_names_file_no_mainfunc( argv[1] );

    // TODO string dgroups_csvfile(argv[2]);
    string basename(argv[2]);

    cout << "Start running PAGC simulator on trace..." << endl;
    FILE* f = fdopen(0, "r");
    string newtrace_filename( basename + "-PAGC-TRACE.csv" );
    ofstream newtrace_file( newtrace_filename );
    unsigned int total_garbage = read_trace_file( f, newtrace_file );
    unsigned int final_time = Exec.NowUp();
    cout << "Done at time " << Exec.NowUp() << endl;
    return 0;
}
