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
// #include "lastmap.h"

// ----------------------------------------------------------------------
// Types
class Object;
class CCNode;

// ----------------------------------------------------------------------
//   Globals

// -- The heap
HeapState Heap;

// -- Execution state
ExecState Exec(2); // Method-only context

// -- Turn on debugging
bool debug = false;

// -- Remember the last event by thread ID
// TODO: Types for object and thread IDs?
// TODO LastMap last_map;

// ----------------------------------------------------------------------
//   Analysis
deque< deque<Object*> > cycle_list;
set<unsigned int> root_set;

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
                // if (target->isLive(Exec.Now())) {
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

    // -- Allocation time
    unsigned int AllocationTime = 0;
    while ( ! tokenizer.isDone()) {
        tokenizer.getLine();
        if (tokenizer.isDone()) {
            break;
        }

        if (Exec.Now() % 1000000 == 1) {
            cout << "  Method time: " << Exec.Now() << "   Alloc time: " << AllocationTime << endl;
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
                    obj = Heap.allocate( tokenizer.getInt(1),
                                         tokenizer.getInt(2),
                                         tokenizer.getChar(0),
                                         tokenizer.getString(3),
                                         as,
                                         els,
                                         thread,
                                         Exec.Now() );
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
                    unsigned int threadId = tokenizer.getInt(5);
                    Thread *thread = Exec.getThread(threadId);
                    Object *oldObj = Heap.get(oldTgtId);
                    obj = Heap.get(objId);
                    target = Heap.get(tgtId);
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
                    }
                    // Increment and decrement refcounts
                    if (obj && target) {
                        unsigned int field_id = tokenizer.getInt(4);
                        Edge* new_edge = Heap.make_edge( obj, field_id,
                                                         target, Exec.Now() );
                        if (thread) {
                            Method *topMethod = thread->TopMethod();
                            if (topMethod) {
                                topMethod->getName();
                            }
                            obj->updateField( new_edge, field_id, Exec.Now(), topMethod, HEAP );
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
                    // 0    1
                    unsigned int objId = tokenizer.getInt(1);
                    obj = Heap.get(objId);
                    if (obj) {
                        unsigned int threadId = tokenizer.getInt(2);
                        Thread *thread = Exec.getThread(threadId);
                        // TODO // Get last event and object from last_map
                        // TODO pair<LastEvent, Object *> last_pair = last_map.getLastEventAndObject( threadId );
                        // TODO LastEvent last_event = last_pair.first;
                        // TODO Object *last_object = last_pair.second;
                        // TODO // Set the object fields
                        // TODO obj->setLastEvent( last_event );
                        // TODO obj->setLastObject( last_object );
                        // TODO if (last_event == LastEvent::ROOT) {
                        // TODO     obj->setDiedByStackFlag();
                        // TODO } else if (last_event == LastEvent::UPDATE) {
                        // TODO     obj->setDiedByHeapFlag();
                        // TODO }
                        Heap->makeDead(obj, Exec.Now());
                        // Get the current method
                        Method *topMethod = NULL;
                        if (thread) {
                            topMethod = thread->TopMethod();
                            if (topMethod) {
                                obj->setDeathSite(topMethod);
                            } 
                        }
                        if (obj->getDiedByStackFlag()) {
                            for ( EdgeMap::iterator p = obj->getEdgeMapBegin();
                                  p != obj->getEdgeMapEnd();
                                  ++p ) {
                                Edge* target_edge = p->second;
                                if (target_edge) {
                                    unsigned int fieldId = target_edge->getSourceField();
                                    obj->updateField( NULL, fieldId, Exec.Now(), topMethod, STACK );
                                }
                            }
                        }
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
                    Object *object = Heap.get(objId);
                    unsigned int threadId = tokenizer.getInt(2);
                    // cout << "objId: " << objId << "     threadId: " << threadId << endl;
                    if (object) {
                        object->setRootFlag(Exec.Now());
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

// ----------------------------------------------------------------------
// Remove edges not in cyclic garbage
void filter_edgelist( deque< pair<int,int> >& edgelist, deque< deque<int> >& cycle_list )
{
    set<int> nodes;
    deque< pair<int,int> > newlist;
    for ( deque< deque<int> >::iterator it = cycle_list.begin();
          it != cycle_list.end();
          ++it ) {
        for ( deque<int>::iterator tmp = it->begin();
              tmp != it->end();
              ++tmp ) {
            nodes.insert(*tmp);
        }
    }
    for ( EdgeList::iterator it = edgelist.begin();
          it != edgelist.end();
          ++it ) {
        set<int>::iterator first_it = nodes.find(it->first);
        if (first_it == nodes.end()) {
            // First node not found, carry on.
            continue;
        }
        set<int>::iterator second_it = nodes.find(it->second);
        if (second_it != nodes.end()) {
            // Found both edge nodes in cycle set 'nodes'
            // Add the edge.
            newlist.push_back(*it);
        }
    }
    edgelist = newlist;
}

// ----------------------------------------------------------------------

int main(int argc, char* argv[])
{
    if (argc != 4) {
        cout << argc << endl;
        cout << "Usage: " << argv[0] << " <namesfile> <output base name> <CYCLE/NOCYCLE>" << endl;
        exit(1);
    }
    string basename(argv[2]);
    string cycle_filename( basename + "-CYCLES.csv" );
    string edge_filename( basename + "-EDGES.txt" );
    string objectinfo_filename( basename + "-OBJECTINFO.txt" );
    string edgeinfo_filename( basename + "-EDGEINFO.txt" );
    string summary_filename( basename + "-SUMMARY.csv" );
    string dsite_filename( basename + "-DSITES.csv" );

    string cycle_switch(argv[3]);
    bool cycle_flag = ((cycle_switch == "NOCYCLE") ? false : true);
    
    cout << "Read names file..." << endl;
    ClassInfo::read_names_file(argv[1]);

    cout << "Start trace..." << endl;
    FILE* f = fdopen(0, "r");
    unsigned int total_objects = read_trace_file(f);
    unsigned int final_time = Exec.Now();
    cout << "Done at time " << Exec.Now() << endl;
    cout << "Total objects: " << total_objects << endl;
    assert( total_objects == Heap.size() );
    Heap.end_of_program(Exec.Now());

    // TODO analyze(Exec.Now());
    // if (true) {
    // TODO Maybe use a finer grained selection of options here.
    //      But for now, doing it this way.
    if (cycle_flag) {
        deque< pair<int,int> > edgelist;
        deque< deque<int> > cycle_list = Heap.scan_queue( edgelist );
        filter_edgelist( edgelist, cycle_list );
        // TODO Heap.analyze();
        cout << "DONE. Getting cycles." << endl;
        set<int> node_set;
        ofstream cycle_file(cycle_filename);
        cycle_file << "---------------[ CYCLES ]-------------------------------------------------------" << endl;
        for ( deque< deque<int> >::iterator it = cycle_list.begin();
              it != cycle_list.end();
              ++it ) {
            for ( deque<int>::iterator tmp = it->begin();
                  tmp != it->end();
                  ++tmp ) {
                cycle_file << *tmp << ",";
                node_set.insert(*tmp);
            }
            cycle_file << endl;
        }
        cycle_file << "---------------[ CYCLES END ]---------------------------------------------------" << endl;
        cycle_file.close();
        ofstream edge_file(edge_filename);
        edge_file << "===============[ EDGES ]========================================================" << endl;
        for ( EdgeList::iterator it = edgelist.begin();
              it != edgelist.end();
              ++it ) {
            edge_file << it->first << " -> " << it->second
                 << endl;
        }
        edge_file << "===============[ EDGES END ]====================================================" << endl;
        edge_file.close();
        ofstream object_info_file(objectinfo_filename);
        object_info_file << "---------------[ OBJECT INFO ]--------------------------------------------------" << endl;
        for ( deque< deque<int> >::iterator it = cycle_list.begin();
              it != cycle_list.end();
              ++it ) {
            for ( deque<int>::iterator tmp = it->begin();
                  tmp != it->end();
                  ++tmp ) {
                Object* object = Heap.get(*tmp);
                object_info_file << *tmp << "," << object->getCreateTime()
                    << "," << object->getDeathTime()
                    << "," << object->getSize()
                    << "," << object->getType()
                    << "," << (object->getDiedByStackFlag() ? "S" : "H")
                    << "," << (object->wasLastUpdateNull() ? "NULL" : "VAL")
                    << "," << (object->getDiedByStackFlag() && object->wasPointedAtByHeap() ? "SHEAP" : "SONLY" )
                    << endl;
            }
        }
        object_info_file << "---------------[ OBJECT INFO END ]----------------------------------------------" << endl;
        object_info_file.close();
        ofstream edge_info_file(edgeinfo_filename);
        edge_info_file << "---------------[ EDGE INFO ]----------------------------------------------------" << endl;
        // srcId, tgtId, allocTime, deathTime
        unsigned int total_edges;
        for ( EdgeSet::iterator it = Heap.begin_edges();
              it != Heap.end_edges();
              ++it ) {
            Edge* eptr = *it;
            Object* source = eptr->getSource();
            Object* target = eptr->getTarget();
            unsigned int srcId = source->getId();
            unsigned int tgtId = target->getId();
            set<int>::iterator srcit = node_set.find(srcId);
            set<int>::iterator tgtit = node_set.find(tgtId);
            if ( (srcit != node_set.end()) || (srcit != node_set.end()) ) {
                edge_info_file << srcId << "," << tgtId << "," << eptr->getCreateTime() << ","
                               << eptr->getEndTime() << endl;
            }
            total_edges++;
        }
        edge_info_file << "---------------[ EDGE INFO END ]------------------------------------------------" << endl;
        edge_info_file.close();
    } else {
        cout << "NOCYCLE chosen. Skipping cycle detection." << endl;
    }
    //     unsigned int getSizeLastUpdateNull() const { return m_totalUpdateNull_size; }
    //     unsigned int getSizeLastUpdateNullHeap() const { return m_totalUpdateNullHeap_size; }
    //     unsigned int getSizeLastUpdateNullStack() const { return m_totalUpdateNullStack_size; }
    //     unsigned int getSizeDiedByStackAfterHeap() const { return m_diedByStackAfterHeap_size; }
    //     unsigned int getSizeDiedByStackOnly() const { return m_diedByStackOnly_size; }
    ofstream summary_file(summary_filename);
    summary_file << "---------------[ SUMMARY INFO ]----------------------------------------------------" << endl;
    summary_file << "number_of_objects," << Heap.size() << endl
                 << "number_of_edges," << Heap.numberEdges() << endl
                 << "died_by_stack," << Heap.getTotalDiedByStack2() << endl
                 << "died_by_heap," << Heap.getTotalDiedByHeap2() << endl
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
                 << "vm_RC_zero," << Heap.getVMObjectsRefCountZero() << endl
                 << "vm_RC_positive," << Heap.getVMObjectsRefCountPositive() << endl
                 << "max_live_size," << Heap.maxLiveSize() << endl
                 << "final_time," << final_time << endl;
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
}

