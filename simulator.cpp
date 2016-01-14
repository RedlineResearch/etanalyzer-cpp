#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <map>
#include <set>
#include <vector>
#include <deque>

using namespace std;

#include "tokenizer.h"
#include "classinfo.h"
#include "execution.h"
#include "heap.h"
#include "refstate.h"

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

// ----------------------------------------------------------------------
//   Analysis
deque< deque<Object*> > cycle_list;

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
                    obj = Heap.get(objId);
                    target = Heap.get(tgtId);
                    // TEMP TODO
                    // Increment and decrement refcounts
                    if (obj && target) {
                        unsigned int field_id = tokenizer.getInt(4);
                        Edge* new_edge = Heap.make_edge( obj, field_id,
                                                         target, Exec.Now() );
                        obj->updateField( new_edge, field_id, Exec.Now() );
                        // DEBUG ONLY IF NEEDED
                        // Example:
                        // if ( (objId == tgtId) && (objId == 166454) ) {
                        // if ( (objId == 166454) ) {
                        //     tokenizer.debugCurrent();
                        // }
                    }
                    // TODO: Why is the old edge not removed?
                }
                break;

            case 'D':
                {
                    // D <object>
                    // 0    1
                    obj = Heap.get(tokenizer.getInt(1));
                    if (obj) {
                        obj->makeDead(Exec.Now());
                        // TEMP TODO this shouldn't decrement refcounts
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
    cout << "Read names file..." << endl;
    ClassInfo::read_names_file(argv[1]);

    cout << "Start trace..." << endl;
    FILE* f = fdopen(0, "r");
    unsigned int total_objects = read_trace_file(f);
    cout << "Done at time " << Exec.Now() << endl;
    cout << "Total objects: " << total_objects << endl;
    Heap.end_of_program(Exec.Now());

    // TODO analyze(Exec.Now());
    deque< pair<int,int> > edgelist;
    deque< deque<int> > cycle_list = Heap.scan_queue( edgelist );
    filter_edgelist( edgelist, cycle_list );
    // TODO Heap.analyze();
    cout << "DONE. Getting cycles." << endl;
    set<int> node_set;
    cout << "---------------[ CYCLES ]-------------------------------------------------------" << endl;
    for ( deque< deque<int> >::iterator it = cycle_list.begin();
          it != cycle_list.end();
          ++it ) {
        for ( deque<int>::iterator tmp = it->begin();
              tmp != it->end();
              ++tmp ) {
            cout << *tmp << ",";
            node_set.insert(*tmp);
        }
        cout << endl;
    }
    cout << "---------------[ CYCLES END ]---------------------------------------------------" << endl;
    cout << "===============[ EDGES ]========================================================" << endl;
    for ( EdgeList::iterator it = edgelist.begin();
          it != edgelist.end();
          ++it ) {
        cout << it->first << " -> " << it->second
             << endl;
    }
    cout << "===============[ EDGES END ]====================================================" << endl;
    cout << "---------------[ OBJECT INFO ]--------------------------------------------------" << endl;
    for ( deque< deque<int> >::iterator it = cycle_list.begin();
          it != cycle_list.end();
          ++it ) {
        for ( deque<int>::iterator tmp = it->begin();
              tmp != it->end();
              ++tmp ) {
            Object* object = Heap.get(*tmp);
            cout << *tmp << "," << object->getCreateTime() << "," << object->getDeathTime()
                 << "," << object->getSize() << "," << object->getType() << endl;
        }
    }
    cout << "---------------[ OBJECT INFO END ]----------------------------------------------" << endl;
    cout << "---------------[ EDGE INFO ]----------------------------------------------------" << endl;
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
            cout << srcId << "," << tgtId << "," << eptr->getCreateTime() << ","
                 << eptr->getEndTime() << endl;
        }
        total_edges++;
    }
    cout << "---------------[ EDGE INFO END ]------------------------------------------------" << endl;
}

