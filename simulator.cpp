#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <map>
#include <set>
#include <vector>

using namespace std;

#include "tokenizer.h"
#include "classinfo.h"
#include "execution.h"
#include "heap.h"

class Object;
class CCNode;

// ----------------------------------------------------------------------
//   Globals

// -- The key object
unsigned int TheKey;

// -- The heap
HeapState Heap;

// -- Execution state
ExecState Exec(2); // Method-only context

// -- Turn on debugging
bool debug = false;

// ----------------------------------------------------------------------
//   Analysis

void sanity_check()
{
          // -- Sanity check
          /*
          const ObjectMap& fields = obj->getFields();
          for (ObjectMap::const_iterator p = fields.begin();
               p != fields.end();
               p++)
            {
              Object * target = (*p).second;
              if (target) {
                if (Now > target->getDeathTime()) {
                  // -- Not good: live object points to a dead object
                  printf(" Live object %s points-to dead object %s\n",
                         obj->info().c_str(), target->info().c_str());
                }
              }                  
            }
          }
          */

        /*
        if (Now > obj->getDeathTime() && obj->getRefCount() != 0) {
          nonzero_ref++;
          printf(" Non-zero-ref-dead %X of type %s\n", obj->getId(), obj->getType().c_str());
        }
        */
}

bool member(Object * obj, const ObjectSet & theset)
{
  return theset.find(obj) != theset.end();
}

int compute_roots(ObjectSet & roots)
{
  unsigned int live = 0;
  for (ObjectMap::iterator p = Heap.begin();
       p != Heap.end();
       p++)
    {
      Object * obj = (*p).second;
      if (obj) {
        if (obj->isLive(Exec.Now())) {
          live++;
          if (obj->getRefCount() == 0)
            roots.insert(obj);
        }
      }
    }

  return live;
}

unsigned int closure(ObjectSet & roots, ObjectSet & premarked, ObjectSet & result)
{
  unsigned int mark_work = 0;

  vector<Object *> worklist;

  // -- Initialize the worklist with only unmarked roots
  for (ObjectSet::iterator i = roots.begin();
       i != roots.end();
       i++)
    {
      Object * root = *i;
      if ( ! member(root, premarked))
        worklist.push_back(root);
    }

  // -- Do DFS until the worklist is empty
  while (worklist.size() > 0) {
    Object * obj = worklist.back();
    worklist.pop_back();
    result.insert(obj);
    mark_work++;

    const EdgeMap& fields = obj->getFields();
    for (EdgeMap::const_iterator p = fields.begin();
         p != fields.end();
         p++)
      {
        Edge * edge = (*p).second;
        Object * target = edge->getTarget();
        if (target) {
          // if (target->isLive(Exec.Now())) {
          if (! member(target, premarked) &&
              ! member(target, result))
            {
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

unsigned int count_live(ObjectSet & objects, unsigned int at_time)
{
  int count = 0;
  // -- How many are actually live
  for (ObjectSet::iterator p = objects.begin();
       p != objects.end();
       p++)
    {
      Object * obj = (*p);
      if (obj->isLive(at_time))
        count++;
    }

  return count;
}


// ----------------------------------------------------------------------
//  Deferred GC simulation

Object * KeyObject = 0;
ObjectSet deferred_objects;
ObjectSet fringe;

void deferred_gc()
{
  if (KeyObject != 0) {
    ObjectSet roots;
    int live = compute_roots(roots);

    cout << "START Deferred GC on " << KeyObject->info() << endl;
    cout << " (0) Premark deferred set size " << deferred_objects.size() << endl;

    cout << " (1) Closure from roots, size " << roots.size() << endl;

    // -- Compute closure from roots, ignoring the deferred objects
    //    (treat deferred objects as "premarked")
    ObjectSet fromroots;
    ObjectSet premarked(deferred_objects);
    int first_pass = closure(roots, premarked, fromroots);

    cout << "     ... marks: " << first_pass << endl;

    cout << " (2) Closure from fringe, size " << fringe.size() << endl;

    // -- Extend the fringe: compute everything reachable from fringe,
    //    stopping at anything not yet marked either way
    //    The trick is constructing the right premark set:
    //        (deferred + fromroots) - fringe
    ObjectSet allmarks(fromroots);
    allmarks.insert(deferred_objects.begin(), deferred_objects.end());
    for (ObjectSet::iterator p = fringe.begin();
         p != fringe.end();
         p++)
      {
        allmarks.erase(*p);
      }
    ObjectSet more_deferred;
    int fringe_extend = closure(fringe, allmarks, more_deferred);

    cout << "     ... marks: " << fringe_extend << endl;

    // -- Add these newly discovered objects to the deferred set
    deferred_objects.insert(more_deferred.begin(), more_deferred.end());

    cout << " (3) Compute new fringe..." << endl;

    // -- Compute the new fringe (also compute "drag")
    fringe.clear();
    unsigned int drag_count = 0;
    for (ObjectSet::iterator p = deferred_objects.begin();
         p != deferred_objects.end();
         p++)
      {
        Object * obj = (*p);
        const EdgeMap& fields = obj->getFields();
        for (EdgeMap::const_iterator q = fields.begin();
             q != fields.end();
             q++)
          {
            Edge * edge = (*q).second;
            Object * target = edge->getTarget();
            if (target && member(target, fromroots)) {
              fringe.insert(target);
            }
          }

        if ( ! obj->isLive(Exec.Now()))
          drag_count++;
      }

    cout << "     ... new fringe size " << fringe.size() << endl;

    cout << " (4) Drag: " << drag_count << " dead out of " << deferred_objects.size() << " deferred objects" << endl;
    cout << "DONE" << endl;
  }
}

// ----------------------------------------------------------------------
//   Read and process trace events

void read_trace_file(FILE * f)
{
  Tokenizer t(f);
  
  unsigned int method_id;
  unsigned int object_id;
  unsigned int target_id;
  unsigned int field_id;
  unsigned int thread_id;
  unsigned int exception_id;
  Object * obj;
  Object * target;
  Method * method;

  // -- Allocation time
  unsigned int AllocationTime = 0;

  while ( ! t.isDone()) {
    t.getLine();
    if (t.isDone()) break;

    if (Exec.Now() % 1000000 == 1)
      cout << "  Method time: " << Exec.Now() << "   Alloc time: " << AllocationTime << endl;

    switch (t.getChar(0)) {
    case 'A':
    case 'I':
    case 'N':
    case 'P':
    case 'V':
      {
        // A/I/N/P/V <id> <size> <type> <site> [<els>] <threadid>
        //     0       1    2      3      4      5         5/6
        unsigned int thrdid = (t.numTokens() == 6) ? t.getInt(5) : t.getInt(6);
        Thread * thread = Exec.getThread(thrdid);
        unsigned int els  = (t.numTokens() == 6) ? 0 : t.getInt(5);
        AllocSite * as = ClassInfo::TheAllocSites[t.getInt(4)];
        obj = Heap.allocate(t.getInt(1), t.getInt(2), t.getChar(0), t.getString(3), as, els, thread, Exec.Now());
        unsigned int old_alloc_time = AllocationTime;
        AllocationTime += obj->getSize();
        /*
        if ((AllocationTime / 1000000) != (old_alloc_time / 1000000)) {
          cout << "STACK TRACE: thread " << thread->getId() << endl;
          cout << thread->stacktrace() << endl;
          deferred_gc();
          AllocationTime = 0;
        }
        */

        /*
        if (obj->getId() == TheKey) {
          // -- Start deferred collection
          KeyObject = obj;
          deferred_objects.insert(obj);
          fringe.insert(obj);
        }
        */
      }
      break;

    case 'U':
      // U <old-target> <object> <new-target> <field> <thread>
      // 0      1          2         3           4        5
      // -- Look up objects and perform update
      obj = Heap.get(t.getInt(2));
      target = Heap.get(t.getInt(3));
      if (obj && target) {
        unsigned int field_id = t.getInt(4);
        Edge * new_edge = Heap.make_edge(obj, field_id, target, Exec.Now());
        // NOTFORCHECKIN obj->updateField(new_edge, Exec.Now());
        /*
        if (member(obj, deferred_objects) && ! member(target, deferred_objects)) {
          if ( ! member(obj, fringe)) {
            cout << "FRINGE add " << obj->info() << endl;
            cout << "       --> " << target->info() << endl;
            fringe.insert(obj);
          }
        }
        */
      }
        
      break;

    case 'D':
      // D <object>
      // 0    1
      obj = Heap.get(t.getInt(1));
      if (obj)
        obj->makeDead(Exec.Now());
      break;

    case 'M':
      // M <methodid> <receiver> <threadid>
      // 0      1         2           3
      // current_cc = current_cc->DemandCallee(method_id, object_id, thread_id);
      method_id = t.getInt(1);
      method = ClassInfo::TheMethods[method_id];
      thread_id = t.getInt(3);
      Exec.Call(method, thread_id);
      break;

    case 'E':
      // E <methodid> <receiver> [<exceptionobj>] <threadid>
      // 0      1         2             3             3/4
      method_id = t.getInt(1);
      method = ClassInfo::TheMethods[method_id];
      thread_id = (t.numTokens() == 4) ? t.getInt(3) : t.getInt(4);
      Exec.Return(method, thread_id);
      break;

    case 'T':
      // T <methodid> <receiver> <exceptionobj>
      // 0      1          2           3
      break;
      
    case 'H':
      // H <methodid> <receiver> <exceptionobj>
      break;
      
    default:
      // cout << "ERROR: Unknown entry " << t.curChar() << endl;
      break;
    }
  }
}

// ----------------------------------------------------------------------

void analyze(unsigned int max_time);

int main(int argc, char * argv[])
{
  cout << "Read names file..." << endl;
  ClassInfo::read_names_file(argv[1]);
  sscanf(argv[2], "%X", &TheKey);
  cout << "Start trace..." << endl;
  FILE * f = fdopen(0, "r");
  read_trace_file(f);
  cout << "Done at time " << Exec.Now() << endl;
  Heap.end_of_program(Exec.Now());
  analyze(Exec.Now());
}

