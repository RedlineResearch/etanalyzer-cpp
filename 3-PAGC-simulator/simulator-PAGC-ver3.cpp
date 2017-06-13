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
struct FunctionRec_t {
    public:
        FunctionRec_t()
            : total(0)
            , minimum(std::numeric_limits<unsigned int>::max())
            , maximum(0)
        {
        }

        void add_garbage( unsigned int garbage )
        {
            this->garbage_vector.push_back( garbage );
            this->total += garbage;
            // TODO: Maybe pass a std::vector or std::map of sub functions
            //       and associated garbage from those sub calls.
            if (garbage < this->minimum) {
                this->minimum = garbage;
            } else if (garbage > this->maximum) {
                this->maximum = garbage;
            }
        };

        unsigned int get_total_garbage() const
        {
            return this->total;
        }

        unsigned int get_minimum() const
        {
            return this->minimum;
        }

        unsigned int get_maximum() const
        {
            return this->maximum;
        }

        unsigned int get_number_methods() const
        {
            return this->garbage_vector.size();
        }

        string gvec2string() const
        {
            string result;
            std::map< unsigned int, unsigned int > counter;
            for ( auto iter = this->garbage_vector.begin();
                  iter != this->garbage_vector.end();
                  iter++ ) {
                auto itmp = counter.find(*iter);
                if (itmp != counter.end()) {
                    counter[*iter]++;
                } else {
                    counter[*iter] = 1;
                }
            }
            for ( auto iter = counter.begin();
                  iter != counter.end();
                  iter++ ) {
                unsigned int key = iter->first;
                unsigned int val = iter->second;
                result.append( std::to_string(key) + ":" + std::to_string(val) + ";" );
            }
            return result;
        }

    private:
        unsigned int total;
        unsigned int minimum;
        unsigned int maximum;
        std::vector< unsigned int > garbage_vector;
};

struct CNode_t {
    public:
        CNode_t( MethodId_t mymethid,
                 CNode_t myparent  )
            : method_id( mymethid )
            , parent( myparent )
            , frec()
        {
            // How to initialize a reference?
        };

        // Use this constructor only for the root node
        CNode_t( MethodId_t mymethid )
            : method_id( mymethid )
            , parent( *this )
            , frec()
        {
            assert( mymethid == 0 );
            // Because only the root node should have no parent. And this way,
            // we assume that all root nodes have method ids of 0.
        };

        inline bool is_root() const
        {
            return (&(this->parent) == this);
        }

        void add( MethodId_t new_id )
        {
            // Check to see if in subtree
            auto iter = subtree.find( new_id );
            if (iter != subtree.end()) {
                // In there already
                // TODO: Is there anything here that needs to be done?
                //       If not, remove this branch. TODO
            } else {
                // Not found. Add it:
                // Allocate new CNode_t
                CNode_t *new_node = new CNode_t( new_id, *this );
                this->subtree[new_id] = new_node;
            }
        };

        CNode_t * find_path( MethodDeque &path )
        {
            // Design decision: Do we assume that
            //     path[0] == this->method_id?
            //     Then, if so, look for path[1] in subtree.
            //     This seems reasonable. - 6 june 2017 RLV
            if ( (path.size() == 0) ||
                 (this->method_id != path[0]->getId()) ) {
                return NULL;
            } else if (path.size() == 1) {
                // So path[0]->getId() == this->method_id,
                // but there's nothing more to the path
                return this;
            } else {
                // So path[0]->getId() == this->method_id,
                // AND there's more to the path.
                // We know there's at least 2 elements in path:
                MethodId_t new_id = path[1]->getId();;
                auto iter = subtree.find( new_id );
                if (iter != subtree.end()) {
                    // Found it:
                    auto path_iter = path.begin();
                    path_iter++; // Go to path[1]
                    MethodDeque subpath( path_iter, path.end() );
                    return iter->second->find_path( subpath );
                } else {
                    // Not found.
                    return NULL;
                }
            }
            // Should't reach here:
            assert(false);
            return NULL;
        };

        bool add_path( MethodDeque &path )
        {
            // TODO: The top of the path should be at the end, not the
            // beginning.
            // We are adding 'path' to the current CNode:
            if (path.size() == 0) {
                // Trivial task of add no path:
                return true;
            } else {
                Method method = *(path.back());
                MethodId_t new_id = method.getId();;
                auto iter = subtree.find( new_id );
                if (iter == subtree.end()) {
                    // Not found.
                    this->add( new_id );
                    iter = subtree.find( new_id );
                }
                // else {
                //     Found it. Simply go down and add the rest of the path:
                // }
                MethodDeque subpath( path.begin(), path.end() );
                subpath.pop_back();
                if (subpath.size() == 0) {
                    // Added everything so:
                    return true;
                } else {
                    // More to go:
                    return iter->second->add_path( subpath );
                }
            }
            // Should't reach here:
            assert(false);
            return NULL;
        };

    private:
        FunctionRec_t frec;
        CNode_t &parent;
        MethodId_t method_id;
        std::map< MethodId_t, CNode_t * > subtree;
};

// A node id is simply an unsigned int
typedef unsigned int NodeId_t;

// TODO: Unused typedefs (probably need to delete) TODO
// TODO: // Simple method counts independent of whether we have thread information
// TODO: // or not
// TODO: typedef std::map< MethodId_t, unsigned int > Method2Count_map_t;
// TODO: // This one uses a context pair of Method pointers:
// TODO: //     (callee, caller)
// TODO: typedef std::map< ContextPair, unsigned int > CPair2Count_map_t;

typedef std::map< CNode_t *, unsigned int > Context2Count_map_t;


typedef std::map< string, std::vector< Summary * > > GroupSum_t;
typedef std::map< string, Summary * > TypeTotalSum_t;
typedef std::map< unsigned int, Summary * > SizeSum_t;

typedef std::map< unsigned int, unsigned int > ObjectMap_t;
// Map from object ID to object size
// ----------------------------------------------------------------------
//   Globals

// -- The pseudo-heap
ObjectMap_t objmap;
// The simple method id to count map
// TODO TMethod2Count_map_t methcount_map;
// The context pair to count map
// CPair2Count_map_t methcount_map;
Context2Count_map_t methcount_map;


// TODO: DELETE HeapState Heap( whereis, keyset );

// -- Execution state
#ifdef ENABLE_TYPE1
ExecMode cckind = ExecMode::Full; // Full calling context
#else
ExecMode cckind = ExecMode::StackOnly; // Stack-only context
#endif // ENABLE_TYPE1

ExecState Exec(cckind);

// Maps from ContextPair -> FunctionRec_t
//  cpair[0] = top/callee
//  cpair[1] = top-1/caller
// NOTE: top-1/caller can be NULL
CNode_t cnode_root( 0 );

// -- Turn on debugging
bool debug = false;

// ----------------------------------------------------------------------
//   Analysis
set<unsigned int> root_set;


// NOT NEEDED? TODO TODO
// This functionality is in the CNode_t class now.
// CNode_t * find_cnode( MethodDeque cpath )
// {
// 
//     return &cnode_root;
// }

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
                    //    CNode_t tree (in cnode_root) when the function exits.
                    auto iter = tid2gstack.find(thread_id);
                    if (iter != tid2gstack.end()) {
                        if (tid2gstack[thread_id].size() > 0) {
                            unsigned int cur = tid2gstack[thread_id].back();
                            tid2gstack[thread_id].pop_back();
                            cur += my_size;
                            tid2gstack[thread_id].push_back( cur );
                        } else {
                            tid2gstack[thread_id].push_back( my_size );
                        }
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
                    Thread *thread;
                    // Get thread
                    if (thread_id > 0) {
                        thread = Exec.getThread(thread_id);
                    } else {
                        // No thread info. Get from ExecState
                        // TODO: Should we just punt here?
                        thread = Exec.get_last_thread();
                    }
                    // Pop off the garbage stack and save in map
                    auto iter = tid2gstack.find(thread_id);
                    unsigned int stack_garbage = 0;
                    if (iter != tid2gstack.end()) {
                        stack_garbage = tid2gstack[thread_id].back();
                        tid2gstack[thread_id].pop_back();
                        // TODO auto iter2 = fnrec_map.find(cpair);
                        // TODO if (iter2 == fnrec_map.end()) {
                        // TODO     FunctionRec_t tmp;
                        // TODO     fnrec_map[cpair] = tmp;
                        // TODO }
                    } else {
                        cerr << "Method EXIT: Empty garbage stack for thread id"
                             << thread_id << "." << endl;
                        //    the else clause shouldn't be possible, but it's worth
                        //    investigating if this happens.
                        //    TODO TODO TODO
                        //    Add some more debugging code if this happens.
                    }
                    if (thread) {
                        MethodDeque path = thread->full_method_stack();
                        if ((path.size()) > 0) {
                            CNode_t *cnode = cnode_root.find_path( path );
                            if (cnode) {
                                auto simpit = methcount_map.find( cnode );
                                if (simpit != methcount_map.end()) {
                                    methcount_map[cnode]++;
                                } else {
                                    methcount_map[cnode] = 1;
                                }
                                // TODO:
                                // Do we output the full path?
                                // dataout << "E," << callee_id << "," << caller_id << endl;
                                // TODO: Probably I should.
                                //------------------------------------------------------------
                                // Save the garbage in the function record
                                // Use stack_garbage
                                // cnode->add_garbage maybe? TODO TODO
                            }
                        }
                    }
                    Exec.Return(method, thread_id);
                    // TODO TODO TODO:
                    // This seems wrong: TODO
                    // tid2gstack[thread_id].push_back(0);
                    // TODO fnrec_map[cpair].add_garbage( stack_garbage );
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

    cout << "Start running PAGC simulator verion 3 on trace..." << endl;
    FILE* f = fdopen(0, "r");
    //-------------------------------------------------------------------------------
    // Output trace csv summary
    string newtrace_filename( basename + "-PAGC-TRACE.csv" );
    ofstream newtrace_file( newtrace_filename );
    unsigned int total_garbage = read_trace_file( f, newtrace_file );
    //-------------------------------------------------------------------------------
    // Output function record summary
    string funcrec_filename( basename + "-PAGC-FUNC.csv" );
    ofstream funcout( funcrec_filename );
    // Output header for the per function CSV file
    //    : Change to method pair.
    //     - First make sure that the method deque has stack discipline. That
    //       is, the lowest method is at the top of the deque at [0]
    funcout << "\"callee_id\",\"caller_id\",\"total_garbage\",\"minimum\",\"maximum\",\"number_times\",\"garbage_list\"" << endl;
    // Output per function record
    /*
    for ( auto iter = fnrec_map.begin();
          iter != fnrec_map.end();
          iter++ ) {
        // output the record:
        //   callee_id, caller_id, total_garbage, minimum, maximum, number_times
        ContextPair cpair = iter->first;
        // Deconstruct the pair
        Method *mptr_callee = cpair.first;
        Method *mptr_caller = cpair.second;
        if (!mptr_callee && !mptr_caller) {
            continue;
        }
        // Else one of these is non-null.
        MethodId_t callee_id = 0;
        if (mptr_callee) {
            callee_id = mptr_callee->getId();
        }
        MethodId_t caller_id = 0;
        if (mptr_caller) {
            caller_id = mptr_caller->getId();
        }
        FunctionRec_t rec = iter->second;
        unsigned int total_garbage = rec.get_total_garbage();
        unsigned int minimum = rec.get_minimum();
        unsigned int maximum = rec.get_maximum();
        unsigned int number = rec.get_number_methods();
        // TODO: So there's something wrong with the methcount_map maybe? TODO
        // TODO TODO TODO TODO TODO TODO
        // Check in simple count map
        unsigned int simple_number;
        auto simpit = methcount_map.find(cpair);
        if (simpit != methcount_map.end()) {
            simple_number = methcount_map[cpair];
        }
        if ( (simpit != methcount_map.end()) &&
             (simple_number != number) ) {
            cerr << "Mismatch: simple[ " << simple_number << " ] != "
                 << " grec[ " << number << " ]." << endl;
        }

        string glist_str = rec.gvec2string();
        funcout << callee_id << ", " << caller_id << ","
                << total_garbage << "," << minimum << "," << maximum << ","
                << simple_number << "," << glist_str
                << endl;
    }
    */
    unsigned int final_time = Exec.NowUp();
    cout << "Done at time " << Exec.NowUp() << endl;
    return 0;
}
