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

typedef struct _GarbageRec_t {
    unsigned int minimum;
    unsigned int maximum;
    double mean;
    double stdev; 
} GarbageRec_t;

typedef std::map< MethodId_t, string > FunctionName_map_t;

// The pseudo-heap data structure
typedef std::map< ObjectId_t, unsigned int > ObjectMap_t;

typedef std::map< MethodId_t, GarbageRec_t > Method2GRec_map_t;
typedef std::map< MethodId_t, Method2GRec_map_t > CPair2GRec_map_t;
// This is the layout of this data structure:
//
// +------------+       +-------------+     +------------------+
// |            |       |             |     |                  |
// |  caller    |       |  callee     |     |  garbage record: |
// |  method Id +-----> |  method Id  +---> |  - minimum       |
// |            |       |             |     |  - maximum       |
// +------------+       +-------------+     |  - mean          |
//                                          |  - std dev       |
//                                          |                  |
//                                          +------------------+
//

// ----------------------------------------------------------------------
//   Globals

// The pseudo-heap
ObjectMap_t objmap;
// The call pair data structure illustrated above
CPair2GRec_map_t cpairmap;
// The names map
FunctionName_map_t namemap;

// TODO: DELETE HeapState Heap( whereis, keyset );

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

unsigned int populate_method_map( string &source_csv,
                                  Method2GRec_map_t &mymap )
{
    std::ifstream infile( source_csv );
    string line;
    // First line is a header:
    std::getline(infile, line);
    // TODO: Maybe make sure we have the right file?
    //       Check the header which should be exactly like this:
    //     header = [ "callee", "caller", "minimum", "mean", "stdev", "maximum",
    //                "called_id", "caller_id", ]
    //     Note: this is Python code.
    int count = 0;
    while (std::getline(infile, line)) {
        GarbageRec_t rec;
        size_t pos = 0;
        string token;
        string s;
        unsigned long int num;
        ++count;
        //------------------------------------------------------------
        // Get the callee
        pos = line.find(",");
        assert( pos != string::npos );
        string callee = line.substr(0, pos);
        // DEBUG: cout << "CALLEE: " << s << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the caller
        pos = line.find(",");
        assert( pos != string::npos );
        string caller = line.substr(0, pos);
        // DEBUG: cout << "CALLER: " << s << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the minimum
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "MIN: " << s << endl;
        rec.minimum = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the mean
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "MIN: " << s << endl;
        rec.mean = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the stdev
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "STDEV: " << s << endl;
        rec.stdev = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the maximum
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "MAX: " << s << endl;
        rec.maximum = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the callee_id
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        // DEBUG: cout << "MAX: " << s << endl;
        int callee_id = std::stoi(s);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the caller_id
        // DEBUG: cout << "MAX: " << line << endl;
        int caller_id = std::stoi(line);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Add the names to the map
        auto niter = namemap.find(caller_id);
        if (niter == namemap.end()) {
            namemap[caller_id] = caller;
        }
        niter = namemap.find(callee_id);
        if (niter == namemap.end()) {
            namemap[callee_id] = callee;
        }
        //------------------------------------------------------------
        // Add to the call pair map
        // First find if caller is in the
        auto iter = cpairmap.find(caller_id);
        if (iter == cpairmap.end()) {
            Method2GRec_map_t *m2g_map = new Method2GRec_map_t();
            cpairmap[caller_id] = *m2g_map;
        }
        cpairmap[caller_id][callee_id] = rec;
    }
    return count;
}

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
                    unsigned int objId = tokenizer.getInt(1);
                    unsigned int my_size = objmap[objId];
                    total_garbage += my_size;
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
        cout << "simulator-PAGC-model-1" << endl
             << "Usage: " << argv[0] << " <PAGC csv filename> <output base name>" << endl
             << "      git version: " <<  build_git_sha << endl
             << "      build date : " <<  build_git_time << endl;
        exit(1);
    }
    // TODO: cout << "Read names file..." << endl;
    // TODO: ClassInfo::read_names_file_no_mainfunc( argv[1] );
    string source_csv(argv[1]);
    Method2GRec_map_t mymap;
    unsigned int result_count = populate_method_map( source_csv,
                                                     mymap );

    cout << "populate count: " << result_count << endl;
    string basename(argv[2]);

    cout << "Start running PAGC simulator on trace..." << endl;
    FILE *f = fdopen(0, "r");
    string out_filename( basename + "-PAGC-MODEL-1.csv" );
    ofstream outfile( out_filename );
    unsigned int total_garbage = read_trace_file( f, outfile );
    unsigned int final_time = Exec.NowUp();
    cout << "Done at time " << Exec.NowUp() << endl;
    return 0;
}
