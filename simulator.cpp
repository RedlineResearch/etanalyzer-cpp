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
ExecState Exec(1); // Full calling context
#else
ExecState Exec(2); // Method-only context
#endif // ENABLE_TYPE1

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
    Object* obj;
    Object* target;
    Method* method;
    unsigned int total_objects;

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
                    Exec.IncUpdateTime();
                    obj = Heap.getObject(objId);
                    target = ((tgtId > 0) ? Heap.getObject(tgtId) : NULL);
                    // TODO last_map.setLast( threadId, LastEvent::UPDATE, obj );
                    if (obj) {
                        obj->setPointedAtByHeap();
                    }
                    if (oldObj) {
                        if (target) {
                            oldObj->unsetLastUpdateNull();
                        } else {
                            oldObj->setLastUpdateNull();
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
                                              UPDATE ); // last event to determine cause
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
                            // Set the death site
                            if (topMethod) {
                                obj->setDeathSite(topMethod);
                            } 
                            if (thread->isLocalVariable(obj)) {
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
                                                          STACK,
                                                          obj,
                                                          OBJECT_DEATH );
                                        // NOTE: STACK is used because the object that died,
                                        // died by STACK.
                                    }
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
        pgs[mytype] = std::vector< Summary * >();
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
                          std::set<ObjectId_t> all_keys )
{
    ofstream object_info_file(objectinfo_filename);
    object_info_file << "---------------[ OBJECT INFO ]--------------------------------------------------" << endl;
    const vector<string> header( { "objId", "createTime", "deathTime", "size", "type",
                                   "diedBy", "lastUpdate", "subCause", "clumpKind",
                                   "deathContext1", "deathContext2", "deathContextType",
                                   "allocSiteName", // "allocContext1", "allocContext2", "allocContextType",
                                   "createTime_alloc", "deathTime_alloc", } );
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
        ContextPair cpair = object->getDeathContextPair();
        Method *meth_ptr1 = std::get<0>(cpair);
        Method *meth_ptr2 = std::get<1>(cpair);
        string method1 = (meth_ptr1 ? meth_ptr1->getName() : "NONAME");
        string method2 = (meth_ptr2 ? meth_ptr2->getName() : "NONAME");
        string allocsite_name = object->getAllocSiteName();
        object_info_file << objId
            << "," << object->getCreateTime()
            << "," << object->getDeathTime()
            << "," << object->getSize()
            << "," << object->getType()
            << "," << dtype
            << "," << (object->wasLastUpdateNull() ? "NULL" : "VAL")
            << "," << (object->getDiedByStackFlag() ? (object->wasPointedAtByHeap() ? "SHEAP" : "SONLY")
                                                    : "H")
            << "," << dgroup_kind
            << "," << method1 // Part 1 of simple context pair - death site
            << "," << method2 // part 2 of simple context pair - death site
            << "," << (object->getDeathContextType() == CPairType::CP_Call ? "C" : "R") // C is call. R is return.
            << "," << allocsite_name
            << "," << object->getCreateTimeAlloc()
            << "," << object->getDeathTimeAlloc()
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
                           string &edgeinfo_filename )
{
    unsigned int total_edges;
    ofstream edge_info_file(edgeinfo_filename);
    edge_info_file << "---------------[ EDGE INFO ]----------------------------------------------------" << endl;
    // srcId, tgtId, allocTime, deathTime
    for ( EdgeSet::iterator it = myheap.begin_edges();
          it != myheap.end_edges();
          ++it ) {
        Edge* eptr = *it;
        Object* source = eptr->getSource();
        Object* target = eptr->getTarget();
        assert(source);
        assert(target);
        unsigned int srcId = source->getId();
        unsigned int tgtId = target->getId();
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
            << eptr->getEndTime() << endl;
        // }
        total_edges++;
    }
    edge_info_file << "---------------[ EDGE INFO END ]------------------------------------------------" << endl;
    edge_info_file.close();
    return total_edges;
}

// ----------------------------------------------------------------------

// Output the map of simple context pair -> count of obects dying
void output_context_summary( string &context_death_count_filename,
                             ExecState &exstate )
{
    ofstream context_death_count_file(context_death_count_filename);
    for ( auto it = exstate.begin_deathCountmap();
          it != exstate.end_deathCountmap();
          ++it ) {
        ContextPair cpair = it->first;
        Method *first = std::get<0>(cpair); 
        Method *second = std::get<1>(cpair); 
        unsigned int total = it->second;
        unsigned int meth1_id = (first ? first->getId() : 0);
        unsigned int meth2_id = (second ? second->getId() : 0);
        string meth1_name = (first ? first->getName() : "NONAME");
        string meth2_name = (second ? second->getName() : "NONAME");
        context_death_count_file << meth1_name << "," 
                                 << meth2_name << ","
                                 << total << endl;
    }
    context_death_count_file.close();
}

// ----------------------------------------------------------------------

int main(int argc, char* argv[])
{
    if (argc != 5) {
        cout << "Usage: " << argv[0] << " <namesfile> <output base name> <CYCLE/NOCYCLE> <OBJDEBUG/NOOBJDEBUG>" << endl;
        cout << "      git version: " << build_git_sha << endl;
        cout << "      build date : " << build_git_time << endl;
        cout << "      CC kind    : " << Exec.get_kind() << endl;
        exit(1);
    }
    cout << "#     git version: " <<  build_git_sha << endl;
    cout << "#     build date : " <<  build_git_time << endl;
    cout << "---------------[ START ]-----------------------------------------------------------" << endl;
    string basename(argv[2]);
    string cycle_filename( basename + "-CYCLES.csv" );
    // string edge_filename( basename + "-EDGES.txt" );
    string objectinfo_filename( basename + "-OBJECTINFO.txt" );
    string edgeinfo_filename( basename + "-EDGEINFO.txt" );
    string summary_filename( basename + "-SUMMARY.csv" );
    string dsite_filename( basename + "-DSITES.csv" );

    string dgroups_filename( basename + "-DGROUPS.csv" );
    string dgroups_by_type_filename( basename + "-DGROUPS-BY-TYPE.csv" );
    string context_death_count_filename( basename + "-CONTEXT-DCOUNT.csv" );

    string call_context_filename( basename + "-CALL-CONTEXT.csv" );
    ofstream call_context_file(call_context_filename);
    Exec.set_output( &call_context_file );
    string nodemap_filename( basename + "-NODEMAP.csv" );
    ofstream nodemap_file(nodemap_filename);
    Exec.set_nodefile( &nodemap_file );

    string cycle_switch(argv[3]);
    bool cycle_flag = ((cycle_switch == "NOCYCLE") ? false : true);
    
    string obj_debug_switch(argv[4]);
    bool obj_debug_flag = ((obj_debug_switch == "OBJDEBUG") ? true : false);
    if (obj_debug_flag) {
        cout << "Enable OBJECT DEBUG." << endl;
        Heap.enableObjectDebug(); // default is no debug
    }

    cout << "Read names file..." << endl;
    ClassInfo::read_names_file(argv[1]);

    cout << "Start trace..." << endl;
    FILE* f = fdopen(0, "r");
    unsigned int total_objects = read_trace_file(f);
    unsigned int final_time = Exec.NowUp();
    unsigned int final_time_alloc = Heap.getAllocTime();
    cout << "Done at update time: " << Exec.NowUp() << endl;
    cout << "Total objects: " << total_objects << endl;
    cout << "Heap.size:     " << Heap.size() << endl;
    // assert( total_objects == Heap.size() );
    Heap.end_of_program(Exec.NowUp());

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
                 << "died_by_global," << Heap.getTotalDiedByHeap2() << endl
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

// TODO:
// void output_all_objects( string &objectinfo_filename,
//                          HeapState &myheap,
//                          std::set<ObjectId_t> dag_keys,
//                          std::set<ObjectId_t> dag_all_set,
//                          std::set<ObjectId_t> all_keys )
// {
//     ofstream object_info_file(objectinfo_filename);
//     object_info_file << "---------------[ OBJECT INFO ]--------------------------------------------------" << endl;
//     for ( ObjectMap::iterator it = myheap.begin();
//           it != myheap.end();
//           ++it ) {
//         Object *object = it->second;
//         ObjectId_t objId = object->getId();
//         set<ObjectId_t>::iterator diter = dag_all_set.find(objId);
//         string dgroup_kind;
//         if (diter == dag_all_set.end()) {
//             // Not a DAG object, therefore CYCLE
//             set<ObjectId_t>::iterator itmp = all_keys.find(objId);
//             dgroup_kind = ((itmp == all_keys.end()) ? "CYC" : "CYCKEY" );
//         } else {
//             // A DAG object
//             set<ObjectId_t>::iterator itmp = dag_keys.find(objId);
//             dgroup_kind = ((itmp == dag_keys.end()) ? "DAG" : "DAGKEY" );
//         }
//         string dtype;
//         if (object->getDiedByStackFlag()) {
//             dtype = "S"; // by stack
//         } else if (object->getDiedByHeapFlag()) {
//             if (object->wasLastUpdateFromStatic()) {
//                 dtype = "G"; // by static global
//             } else {
//                 dtype = "H"; // by heap
//             }
//         } else {
//             dtype = "E"; // program end
//         }
//         ContextPair cpair = object->getDeathContextPair();
//         Method *meth_ptr1 = std::get<0>(cpair);
//         Method *meth_ptr2 = std::get<1>(cpair);
//         string method1 = (meth_ptr1 ? meth_ptr1->getName() : "NONAME");
//         string method2 = (meth_ptr2 ? meth_ptr2->getName() : "NONAME");
//         string allocsite_name = object->getAllocSiteName();
//         object_info_file << objId
//             << "," << object->getCreateTime()
//             << "," << object->getDeathTime()
//             << "," << object->getSize()
//             << "," << object->getType()
//             << "," << dtype
//             << "," << (object->wasLastUpdateNull() ? "NULL" : "VAL")
//             << "," << (object->getDiedByStackFlag() ? (object->wasPointedAtByHeap() ? "SHEAP" : "SONLY")
//                                                     : "H")
//             << "," << dgroup_kind
//             << "," << method1 // Part 1 of simple context pair
//             << "," << method2 // part 2 of simple context pair
//             << "," << allocsite_name
//             << "," << object->getCreateTimeAlloc()
//             << "," << object->getDeathTimeAlloc()
//             << endl;
//             // TODO: The following can be made into a lookup table:
//             //       method names
//             //       allocsite names
//             //       type names
//             // May only be necessary for performance reasons (ie, simulator eats up too much RAM 
//             // on the larger benchmarks/programs.)
//     }
//     object_info_file << "---------------[ OBJECT INFO END ]----------------------------------------------" << endl;
//     object_info_file.close();
// }
