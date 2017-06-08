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
// Utility functions

inline double mean( std::vector< unsigned int > data )
{
    return ( std::accumulate( data.begin(),
                              data.end(),
                              0.0 )
             / data.size() );
}

inline double stdev( std::vector< unsigned int > data,
                     double mymean )
{
    double sqsum = std::inner_product( data.begin(),
                                       data.end(),
                                       data.begin(),
                                       0.0 );
    return std::sqrt( (sqsum / data.size())
                      - (mymean * mymean) );
}

inline double stdev( std::vector< unsigned int > data )
{
    double mymean = mean( data );
    return stdev( data, mymean );
}

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
    unsigned int number;
    unsigned int minimum;
    unsigned int maximum;
    double mean;
    double stdev; 
    unsigned int total_garbage;
    string garbage_list;
} GarbageRec_t;

// Function id to name map
typedef std::map< MethodId_t, string > FunctionName_map_t;

// The pseudo-heap data structure
typedef std::map< ObjectId_t, unsigned int > ObjectMap_t;

// Garbage history map from:
//     time -> garbage pair
// NOTE: Time can be any valid time unit (alloc, method, garbology)
typedef std::map< unsigned int, GPair_t > GarbageHistory_t;

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
// TODO: // The names map
// TODO: FunctionName_map_t namemap;

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

//-------------------------------------------------------------------------------
//
unsigned int process_garbage_lists( CPair2GRec_map_t &mycpairmap )
{
    int count = 0;
    for ( auto iter = mycpairmap.begin();
          iter != mycpairmap.end();
          iter++ ) {
        MethodId_t caller_id = iter->first;
        Method2GRec_map_t &m2g = iter->second;
        for ( auto it2 = m2g.begin();
              it2 != m2g.end();
              it2++ ) {
            std::vector< unsigned int > gveclist;
            MethodId_t callee_id = it2->first;
            GarbageRec_t &rec = it2->second;
            string glist = rec.garbage_list;
            size_t pos = 0;
            string token;
            string s;
            // The first level separator is a semicolon
            pos = glist.find(";");
            // Assert that the garbage list isn't empty.
            assert( pos != string::npos );
            while (pos != string::npos) {
                // Then split on a colon
                size_t cpos = glist.find(":");
                unsigned int garbage = std::stoi(glist.substr(0, cpos));
                unsigned int number = std::stoi(glist.substr(cpos + 1, pos));
                assert( number > 0 );
                // Insert into vector
                gveclist.insert( gveclist.end(), number, garbage );
                glist.erase(0, pos + 1);
                pos = glist.find(";");
            }
            // TODO: Find mean and std deviation.
            // rec.mean = 0.0;
            // rec.stdev = 0.0;
            double curmean = mean( gveclist );
            double curstdev = stdev( gveclist, curmean );
            rec.mean = curmean;
            rec.stdev = curstdev;
        }
    }
    return count;
}

//-------------------------------------------------------------------------------
//
unsigned int populate_method_map( string &source_csv,
                                  CPair2GRec_map_t &mycpairmap )
{
    std::ifstream infile( source_csv );
    string line;
    // First line is a header:
    std::getline(infile, line);
    // TODO: Maybe make sure we have the right file? TODO
    //       Check the header which should be exactly like this:
    //      header = [ "callee_id", "caller_id", "number", "garbage", "garbage_list", ]
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
        unsigned int callee_id = std::stoi(line.substr(0, pos));
        // DEBUG: cout << "CALLEE: " << callee_id << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the caller
        pos = line.find(",");
        assert( pos != string::npos );
        unsigned int caller_id = std::stoi(line.substr(0, pos));
        // DEBUG: cout << "CALLER: " << caller_id << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the number
        pos = line.find(",");
        assert( pos != string::npos );
        s = line.substr(0, pos);
        rec.number = std::stoi(s);
        // DEBUG: cout << "NUMBER: " << rec.number << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the garbage 
        pos = line.find(",");
        assert( pos != string::npos );
        rec.total_garbage = std::stoi(line.substr(0, pos));
        // DEBUG: cout << "GARBAGE: " << rec.total_garbage << endl;
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the garbage list
        pos = line.find(",");
        assert( pos != string::npos );
        rec.garbage_list = line.substr(0, pos);
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the mean
        pos = line.find(",");
        assert( pos != string::npos );
        rec.mean = std::stof(line.substr(0, pos));
        line.erase(0, pos + 1);
        //------------------------------------------------------------
        // Get the standard deviation
        rec.mean = std::stof(line);
        //------------------------------------------------------------
        // Add to the call pair map
        // First find if caller is in the
        auto iter = mycpairmap.find(caller_id);
        if (iter == mycpairmap.end()) {
            Method2GRec_map_t *m2g_map = new Method2GRec_map_t();
            mycpairmap[caller_id] = *m2g_map;
        }
        mycpairmap[caller_id][callee_id] = rec;
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
                    unsigned int curtime = Exec.NowUp();
                    method_id = tokenizer.getInt(1);
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
                        thread = Exec.get_last_thread();
                    }
                    if (thread) {
                        MethodDeque top2meth = thread->top_N_methods(2);
                        // These are Method pointers.
                        if (top2meth[0] && top2meth[1]) {
                            MethodId_t callee_id = top2meth[0]->getId();
                            MethodId_t caller_id = top2meth[1]->getId();
                            auto it_caller = cpairmap.find(caller_id);
                            if (it_caller != cpairmap.end()) {
                                // Found the caller. Look for callee:
                                Method2GRec_map_t m2g = it_caller->second;
                                if (caller_id > 0) {
                                    auto it_callee = m2g.find(callee_id);
                                    if (it_callee != m2g.end()) {
                                        // Found the callee too.
                                        GarbageRec_t rec = it_callee->second;
                                        // Save the actual and estimated garbage only if
                                        // we have a new estimate.
                                        estimate += rec.mean;
                                        ghist[curtime] = make_pair( total_garbage, estimate );
                                        timevec.push_back(curtime);
                                    }
                                }
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
void debug_method_map( CPair2GRec_map_t &mymap )
{
    cout << "DEBUG method-map:" << endl;
    for ( auto it1 = mymap.begin();
          it1 != mymap.end();
          it1++ ) {
        MethodId_t caller_id = it1->first;
        Method2GRec_map_t &m2gmap = it1->second;
        // TODO: string caller = namemap[caller_id];
        cout << "caller[ " << caller_id << " ]" << endl;
        for ( auto it2 = m2gmap.begin();
              it2 != m2gmap.end();
              it2++ ) {
            MethodId_t callee_id = it2->first;
            // TODO: string callee = namemap[callee_id];
            cout << "  - callee[ " << callee_id << " ] -> " << endl;
        }
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
                                                     cpairmap );
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
    // TODO: This call should go BEFORE read_trace_file, but what should we do
    // for the records? Populate instead of assuming they exist. TODO
    process_garbage_lists( cpairmap );

    debug_method_map( cpairmap );
    string ghist_out_filename( basename + "-PAGC-MODEL-1-timeseries.csv" );
    output_garbage_history( ghist_out_filename,
                            ghist,
                            timevec );
    unsigned int final_time = Exec.NowUp();
    cout << "Done at time " << Exec.NowUp() << endl;
    return 0;
}
