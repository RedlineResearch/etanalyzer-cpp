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

// ----------------------------------------------------------------------
// Garbage pair
//    first is actual
//    second is estimate
typedef std::pair< unsigned int, unsigned int > GPair_t;

inline unsigned int get_actual( GPair_t gp )
{
    return gp.first;
}

inline unsigned int get_estimate( GPair_t gp )
{
    return gp.second;
}

// ----------------------------------------------------------------------
// Garbage record
typedef struct _GarbageRec_t {
    unsigned int minimum;
    unsigned int maximum;
    double mean;
    double stdev; 
} GarbageRec_t;

// Function id to name map
typedef std::map< MethodId_t, string > FunctionName_map_t;

// The pseudo-heap data structure
typedef std::map< ObjectId_t, unsigned int > ObjectMap_t;

// Simple method counts independent of whether we have thread information
// or not
typedef std::map< MethodId_t, unsigned int > Method2Count_map_t;

typedef std::map< MethodId_t, GarbageRec_t > Method2GRec_map_t;
// Garbage history map from:
//     time -> garbage pair
// NOTE: Time can be any valid time unit (alloc, method, garbology)
typedef std::map< unsigned int, GPair_t > GarbageHistory_t;

typedef std::map< MethodId_t, GarbageRec_t > Method2GRec_map_t;
// TODO: typedef std::map< MethodId_t, Method2GRec_map_t > CPair2GRec_map_t;
// This is the layout of this data structure:
//
// +------------+      +------------------+
// |            |      |                  |
// |            |      |  garbage record: |
// |  method Id +----->|  - minimum       |
// |            |      |  - maximum       |
// +------------+      |  - mean          |
//                     |  - std dev       |
//                     |                  |
//                     +------------------+
//

// ----------------------------------------------------------------------
//   Globals

// The pseudo-heap
ObjectMap_t objmap;

// The method to garbage record map
Method2GRec_map_t methmap;
// The call pair data structure illustrated above
// TODO CPair2GRec_map_t cpairmap;
// The simple method id to count map
Method2Count_map_t methcount_map;
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
                                  Method2GRec_map_t &mymethmap,
                                  FunctionName_map_t &mynamemap )
{
    std::ifstream infile( source_csv );
    string line;
    // First line is a header:
    std::getline(infile, line);
    // TODO: Maybe make sure we have the right file?
    //       Check the header which should be exactly like this:
    //     header = [ "method_id", "number", "garbage", "garbage_list", ]
    //     Note: this is Python code.
    int count = 0;
    while (std::getline(infile, line)) {
        GarbageRec_t rec;
        size_t pos = 0;
        string token;
        string s;
        unsigned long int num;
        ++count;
        cout << "-------------------------------------------------------------------------------" << endl;
        //------------------------------------------------------------
        // Get the method_id
        pos = line.find(",");
        assert( pos != string::npos );
        string method_id_str = line.substr(0, pos);
        unsigned int method_id = std::stoi(method_id_str);
        cout << "METHOD_ID: " << method_id << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the number
        pos = line.find(",");
        assert( pos != string::npos );
        string number_str = line.substr(0, pos);
        unsigned int number = std::stoi(number_str);
        cout << "NUMBER: " << number << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the garbage
        pos = line.find(",");
        assert( pos != string::npos );
        string garbage_str = line.substr(0, pos);
        unsigned int garbage = std::stoi(garbage_str);
        cout << "GARBAGE : " << garbage << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the garbage list
        assert( pos != string::npos );
        string garbage_list = s;
        cout << "GARBAGE_LIST : " << garbage_list << endl;
        // TODO HERE TODO 2 June 2017 TODO
        //------------------------------------------------------------
        // Add the names to the map
        auto methiter = ClassInfo::TheMethods.find(method_id);
        assert( methiter != ClassInfo::TheMethods.end() );
        Method *mptr = ClassInfo::TheMethods[method_id];
        assert( mptr != NULL );
        string methname = mptr->getName();
        auto niter = mynamemap.find(method_id);
        if (niter == mynamemap.end()) {
            mynamemap[method_id] = methname;
        }
        //------------------------------------------------------------
        // Add to the call pair map
        // First find if caller is in the
        auto iter = mymethmap.find(method_id);
        GarbageRec_t tmp;
        mymethmap[method_id] = tmp;
    }
    return count;
}

//-------------------------------------------------------------------------------
//
unsigned int output_garbage_history( string &ghist_out_filename,
                                     GarbageHistory_t &ghist,
                                     std::vector< VTime_t > &timevec )
{
    ofstream ghout( ghist_out_filename );
    ghout << "'time','actual','estimate'" << endl;
    for ( auto itvec = timevec.begin();
          itvec != timevec.end();
          itvec++ ) {
        VTime_t curtime = *itvec;
        ghout << curtime << ","
              << ghist[curtime].first << ","
              << ghist[curtime].second << endl;
    }
    return 0;
}

//-------------------------------------------------------------------------------
//
unsigned int read_trace_file( FILE *f,
                              ofstream &dataout,
                              GarbageHistory_t &ghist,
                              std::vector< VTime_t > &timevec )
// TODO:????
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
    unsigned int estimate = 0;

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
                    unsigned int my_id = tokenizer.getInt(1);
                    unsigned int my_size = tokenizer.getInt(2);
                    objmap[my_id] = my_size;
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
                    unsigned int curtime = Exec.NowUp();
                    method_id = tokenizer.getInt(1);
                    // Save in simple count map
                    auto simpit = methcount_map.find(method_id);
                    if (simpit != methcount_map.end()) {
                        methcount_map[method_id]++;
                    } else {
                        methcount_map[method_id] = 1;
                    }
                    method = ClassInfo::TheMethods[method_id];
                    thread_id = (tokenizer.numTokens() == 4) ? tokenizer.getInt(3)
                                                             : tokenizer.getInt(4);

                    Thread *thread;
                    if (thread_id > 0) {
                        thread = Exec.getThread(thread_id);
                        // Update counters in ExecState for map of
                        //   Object * to simple context pair
                    } else {
                        // No thread info. Get from ExecState
                        // TODO: Should we just punt here?
                        thread = Exec.get_last_thread();
                    }
                    if (thread) {
                        // MethodDeque top2meth = thread->top_N_methods(2);
                        Method *topmeth = thread->TopMethod();
                        // These are Method pointers.
                        // TODO if (top2meth[0] && top2meth[1]) {
                        if (topmeth) {
                            MethodId_t methid = topmeth->getId();
                            auto itmp = methmap.find(methid);
                            if (itmp != methmap.end()) {
                                GarbageRec_t rec = itmp->second;
                                // Save the actual and estimated garbage only if
                                // we have a new estimate.
                                estimate += rec.mean;
                                ghist[curtime] = make_pair( total_garbage, estimate );
                                timevec.push_back(curtime);
                            }
                        }
                    }

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
void debug_method_map( Method2GRec_map_t methmap )
{
    cout << "DEBUG method-map:" << endl;
    for ( auto it = methmap.begin();
          it != methmap.end();
          it++ ) {
        MethodId_t method_id = it->first;
        string method = namemap[method_id];
        cout << "  - method[ " << method << " ] -> " << endl;
    }
}

// ----------------------------------------------------------------------
// 
int main(int argc, char* argv[])
{
    if (argc != 4) {
        cout << "simulator-PAGC-model-1" << endl
             << "Usage: " << argv[0] << " <names filename>  <PAGC csv filename>  <output base name>" << endl
             << "      git version: " <<  build_git_sha << endl
             << "      build date : " <<  build_git_time << endl;
        exit(1);
    }
    cout << "Read names file..." << endl;
    ClassInfo::read_names_file_no_mainfunc( argv[1] );
    string source_csv(argv[2]);
    string basename(argv[3]);
    //
    // Read in the method map
    unsigned int result_count = populate_method_map( source_csv,
                                                     methmap,
                                                     namemap );
    debug_method_map( methmap );
    cout << "populate count: " << result_count << endl;


    cout << "Start running PAGC simulator on trace..." << endl;
    FILE *f = fdopen(0, "r");
    string out_filename( basename + "-PAGC-MODEL-1.csv" );
    ofstream outfile( out_filename );
    GarbageHistory_t ghist;
    std::vector< VTime_t > timevec;
    unsigned int total_garbage = read_trace_file( f,
                                                  outfile,
                                                  ghist,
                                                  timevec );
    string ghist_out_filename( basename + "-PAGC-MODEL-1-timeseries.csv" );
    output_garbage_history( ghist_out_filename,
                            ghist,
                            timevec );
    unsigned int final_time = Exec.NowUp();
    cout << "Done at time " << Exec.NowUp() << endl;
    return 0;
}
