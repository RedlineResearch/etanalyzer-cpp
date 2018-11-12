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
// #include "refstate.h"
// #include "summary.hpp"
#include "version.hpp"

// ----------------------------------------------------------------------
// Types
// TODO DELETE TODO class Object;
// TODO DELETE TODO class CCNode;
typedef std::map< unsigned int, unsigned int > ObjIdMap;

// ----------------------------------------------------------------------
//   Globals

ExecState Exec(ExecMode::StackOnly);

// -- Turn on debugging
bool debug = false;

// ----------------------------------------------------------------------
//   Analysis

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

    unsigned int object_id;
    unsigned int total_objects = 0;
    ObjIdMap livemap;
    unsigned int long curlive;
    unsigned int long maxlive;
    // UNUSED CODE. TODO: Here just in case, but probably deleted later
    // unsigned int target_id;
    // unsigned int field_id;
    // unsigned int thread_id;
    // unsigned int exception_id;
    // unsigned int method_id;
    // Object *obj;
    // Object *target;
    // Method *method;

    // -- Allocation time
    unsigned int AllocationTime = 0;
    while ( !tokenizer.isDone() ) {
        tokenizer.getLine();
        if (tokenizer.isDone()) {
            break;
        }
        if (Exec.NowUp() % 1000000 == 1) {
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
                    // TODO unsigned int thrdid = (tokenizer.numTokens() == 6) ? tokenizer.getInt(6)
                    // TODO                                                    : tokenizer.getInt(5);
                    // TODO // TODO Thread* thread = Exec.getThread(thrdid);
                    // TODO unsigned int els  = (tokenizer.numTokens() == 6) ? tokenizer.getInt(5)
                    // TODO                                                  : 0;
                    // TODO AllocSite* as = ClassInfo::TheAllocSites[tokenizer.getInt(4)];
                    // TODO unsigned int old_alloc_time = AllocationTime;
                    // TODO AllocationTime += obj->getSize();
                    total_objects++;
                    // TODO:
                    unsigned int objId = tokenizer.getInt(1);
                    unsigned int size = tokenizer.getInt(2);
                    // 1. Add to live set if not a dupe
                    ObjIdMap::iterator iter = livemap.find(objId);
                    if (iter == livemap.end()) {
                        // Found it
                        livemap[objId] = size;
                        // 2. Increase current live size
                        curlive += size;
                        // 3. Increase max live size if greater than current max
                        maxlive = (curlive > maxlive ? curlive : maxlive);
                    }
                    // ELSE it's a dupe, so ignore it
                }
                break;

            case 'D':
                {
                    // D <object> <thread-id>
                    // 0    1
                    unsigned int objId = tokenizer.getInt(1);
                    // 1. Check live set
                    ObjIdMap::iterator iter = livemap.find(objId);
                    if (iter != livemap.end()) {
                        // Found it
                        curlive -= iter->second;
                    }
                    // 2. Increase current live size
                    // 3. Increase max live size if greater than current max
                    // TODO:
                    // 1. Check if in live set
                    // 2. Decrease current live size if in.
                    // 3. Ignore if not and add to ignored total.
                }
                break;

            // All other events after this is ignored.
            case 'U':
                {
                    // U <old-target> <object> <new-target> <field> <thread>
                    // 0      1          2         3           4        5
                    // -- Look up objects and perform update
                    // TODO unsigned int objId = tokenizer.getInt(2);
                    // TODO unsigned int tgtId = tokenizer.getInt(3);
                    // TODO unsigned int oldTgtId = tokenizer.getInt(1);
                    // TODO unsigned int threadId = tokenizer.getInt(5);
                    // TODO Thread *thread = Exec.getThread(threadId);
                    // TODO target = ((tgtId > 0) ? Heap.getObject(tgtId) : NULL);
                }
                break;

            case 'M':
                {
                    // M <methodid> <receiver> <threadid>
                    // 0      1         2           3
                    // current_cc = current_cc->DemandCallee(method_id, object_id, thread_id);
                    // TEMP TODO ignore method events
                    // method_id = tokenizer.getInt(1);
                    // method = ClassInfo::TheMethods[method_id];
                    // thread_id = tokenizer.getInt(3);
                    // Exec.Call(method, thread_id);
                }
                break;

            case 'E':
            case 'X':
                {
                    // E <methodid> <receiver> [<exceptionobj>] <threadid>
                    // 0      1         2             3             3/4
                    // TODO method_id = tokenizer.getInt(1);
                    // TODO method = ClassInfo::TheMethods[method_id];
                    // TODO thread_id = (tokenizer.numTokens() == 4) ? tokenizer.getInt(3)
                    // TODO                                          : tokenizer.getInt(4);
                    // TODO Exec.Return(method, thread_id);
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
                }
                break;

            default:
                // cout << "ERROR: Unknown entry " << tokenizer.curChar() << endl;
                break;
        }
    }
    return maxlive;
}

// ----------------------------------------------------------------------

int main(int argc, char* argv[])
{
    if (argc != 2) {
        cout << "Usage: " << argv[0] << " <namesfile>" << endl;
        cout << "      git version: " <<  build_git_sha << endl;
        cout << "      build date : " <<  build_git_time << endl;
        exit(1);
    }

    cout << "Read names file..." << endl;
    ClassInfo::read_names_file_no_mainfunc( argv[1] );

    cout << "Start trace..." << endl;
    FILE *f = fdopen(0, "r");
    unsigned int max_livesize = read_trace_file(f);
    cout << "Max livesize: " << max_livesize << endl;
}

