#ifndef HEAP_H
#define HEAP_H

// ----------------------------------------------------------------------
//   Representation of objects on the heap
//
#include <algorithm>
#include <iostream>
#include <map>
#include <deque>
#include <limits.h>
#include <assert.h>
#include <boost/logic/tribool.hpp>

#include "classinfo.h"
#include "refstate.h"

class Object;
class Thread;
class Edge;

enum Reason {
    STACK = 1,
    HEAP = 2,
    UNKNOWN_REASON = 99,
};

enum LastEvent {
    ROOT = 1,
    UPDATE = 2,
    UNKNOWN_EVENT = 99,
};

typedef map<unsigned int, Object *> ObjectMap;
typedef map<unsigned int, Edge *> EdgeMap;
typedef set<Object *> ObjectSet;
typedef set<Edge *> EdgeSet;
typedef deque< pair<int,int> > EdgeList;

typedef map<Method *, set<string> *> DeathSitesMap;
// Where we save the method death sites. This has to be method pointer
// to:
// 1) set of object pointers
// 2) set of object types
// 2 seems better.
using namespace boost::logic;

class HeapState
{
    public:

        // -- Do ref counting?
        static bool do_refcounting;

        // -- Turn on debugging
        static bool debug;

        // -- Turn on output of objects to stdout
        bool m_obj_debug_flag;

    private:
        // -- Map from IDs to objects
        ObjectMap m_objects;

        // -- Set of edges (all pointers)
        EdgeSet m_edges;

        // Map from IDs to bool if possible cyle root
        map<unsigned int, bool> m_candidate_map;

        unsigned long int m_liveSize; // current live size of program in bytes
        unsigned long int m_maxLiveSize; // max live size of program in bytes

        // Total number of objects that died by loss of heap reference version 2
        unsigned int m_totalDiedByHeap_ver2;
        // Total number of objects that died by stack frame going out of scope version 2
        unsigned int m_totalDiedByStack_ver2;
        // Total number of objects unknown using version 2 method
        unsigned int m_totalDiedUnknown_ver2;
        // Size of objects that died by loss of heap reference
        unsigned int m_sizeDiedByHeap;
        // Size of objects that died by stack frame going out of scope
        unsigned int m_sizeDiedByStack;

        // Total number of objects whose last update away from the object
        // was null
        unsigned int m_totalUpdateNull;
        //    -- that was part of the heap
        unsigned int m_totalUpdateNullHeap;
        //    -- that was part of the stack
        unsigned int m_totalUpdateNullStack;
        // Size of objects whose last update away from the object was null
        unsigned int m_totalUpdateNull_size;
        //    -- that was part of the heap
        unsigned int m_totalUpdateNullHeap_size;
        //    -- that was part of the stack
        unsigned int m_totalUpdateNullStack_size;

        // Died by stack with previous heap action
        unsigned int m_diedByStackAfterHeap;
        // Died by stack only
        unsigned int m_diedByStackOnly;
        // Died by stack with previous heap action -- size
        unsigned int m_diedByStackAfterHeap_size;
        // Died by stack only -- size
        unsigned int m_diedByStackOnly_size;

        // Number of objects with no death sites
        unsigned int m_no_dsites_count;
        // Number of VM objects that always have RC 0
        unsigned int m_vm_refcount_0;
        // Number of VM objects that have RC > 0 at some point
        unsigned int m_vm_refcount_positive;

        // Map of Method * to set of object type names
        DeathSitesMap m_death_sites_map;

        void update_death_counters( Object *obj );
        Method * get_method_death_site( Object *obj );

    public:
        HeapState()
            : m_objects()
            , m_candidate_map()
            , m_death_sites_map()
            , m_maxLiveSize(0)
            , m_liveSize(0)
            , m_totalDiedByHeap_ver2(0)
            , m_totalDiedByStack_ver2(0)
            , m_sizeDiedByHeap(0)
            , m_sizeDiedByStack(0)
            , m_totalDiedUnknown_ver2(0)
            , m_totalUpdateNull(0)
            , m_totalUpdateNullHeap(0)
            , m_totalUpdateNullStack(0)
            , m_totalUpdateNull_size(0)
            , m_totalUpdateNullHeap_size(0)
            , m_totalUpdateNullStack_size(0)
            , m_diedByStackAfterHeap(0)
            , m_diedByStackOnly(0)
            , m_no_dsites_count(0)
            , m_vm_refcount_positive(0)
            , m_vm_refcount_0(0)
            , m_obj_debug_flag(false) {
        }

        void enableObjectDebug() { m_obj_debug_flag = true; }
        void disableObjectDebug() { m_obj_debug_flag = false; }

        Object* allocate( unsigned int id,
                          unsigned int size,
                          char kind,
                          char* type,
                          AllocSite* site, 
                          unsigned int els,
                          Thread* thread,
                          unsigned int create_time );

        Object* get(unsigned int id);

        Edge* make_edge( Object* source, unsigned int field_id, Object* target, unsigned int cur_time);

        void makeDead(Object * obj, unsigned int death_time);

        ObjectMap::iterator begin() { return m_objects.begin(); }
        ObjectMap::iterator end() { return m_objects.end(); }
        unsigned int size() const { return m_objects.size(); }
        unsigned long int liveSize() const { return m_liveSize; }
        unsigned long int maxLiveSize() const { return m_maxLiveSize; }

        unsigned int getTotalDiedByStack2() const { return m_totalDiedByStack_ver2; }
        unsigned int getTotalDiedByHeap2() const { return m_totalDiedByHeap_ver2; }
        unsigned int getTotalDiedUnknown() const { return m_totalDiedUnknown_ver2; }
        unsigned int getSizeDiedByHeap() const { return m_sizeDiedByHeap; }
        unsigned int getSizeDiedByStack() const { return m_sizeDiedByStack; }

        unsigned int getTotalLastUpdateNull() const { return m_totalUpdateNull; }
        unsigned int getTotalLastUpdateNullHeap() const { return m_totalUpdateNullHeap; }
        unsigned int getTotalLastUpdateNullStack() const { return m_totalUpdateNullStack; }
        unsigned int getSizeLastUpdateNull() const { return m_totalUpdateNull_size; }
        unsigned int getSizeLastUpdateNullHeap() const { return m_totalUpdateNullHeap_size; }
        unsigned int getSizeLastUpdateNullStack() const { return m_totalUpdateNullStack_size; }

        unsigned int getDiedByStackAfterHeap() const { return m_diedByStackAfterHeap; }
        unsigned int getDiedByStackOnly() const { return m_diedByStackOnly; }
        unsigned int getSizeDiedByStackAfterHeap() const { return m_diedByStackAfterHeap_size; }
        unsigned int getSizeDiedByStackOnly() const { return m_diedByStackOnly_size; }

        unsigned int getNumberNoDeathSites() const { return m_no_dsites_count; }

        unsigned int getVMObjectsRefCountZero() const { return m_vm_refcount_0; }
        unsigned int getVMObjectsRefCountPositive() const { return m_vm_refcount_positive; }

        DeathSitesMap::iterator begin_dsites() { return m_death_sites_map.begin(); }
        DeathSitesMap::iterator end_dsites() { return m_death_sites_map.end(); }

        void add_edge(Edge* e) { m_edges.insert(e); }
        EdgeSet::iterator begin_edges() { return m_edges.begin(); }
        EdgeSet::iterator end_edges() { return m_edges.end(); }
        unsigned int numberEdges() { return m_edges.size(); }

        void end_of_program(unsigned int cur_time);

        void set_candidate(unsigned int objId);
        void unset_candidate(unsigned int objId);
        deque< deque<int> > scan_queue( EdgeList& edgelist );
        void set_reason_for_cycles( deque< deque<int> >& cycles );
};

enum Color {
    BLUE = 1,
    RED = 2,
    PURPLE = 3, // UNUSED
    BLACK = 4,
    GREEN = 5,
};

class Object
{
    private:
        unsigned int m_id;
        unsigned int m_size;
        char m_kind;
        string m_type;
        AllocSite* m_site;
        unsigned int m_elements;
        Thread* m_thread;

        unsigned int m_createTime;
        unsigned int m_deathTime;

        unsigned int m_refCount;
        Color m_color;
        ObjectRefState m_refState;
        unsigned int m_maxRefCount;

        EdgeMap m_fields;

        HeapState* m_heapptr;

        // Was this object ever a target of a heap pointer?
        bool m_pointed_by_heap;
        // Was this object ever a root?
        bool m_was_root;
        // Did last update move to NULL?
        tribool m_last_update_null; // If false, it moved to a differnet object
        // Did this object die by loss of heap reference?
        bool m_diedByHeap;
        // Did this object die by loss of stack reference?
        bool m_diedByStack;
        // Reason for death
        Reason m_reason;
        // Time that m_reason happened
        unsigned int m_last_action_time;
        // Method where this object died
        Method *m_methodDeathSite;
        // Last method to decrement reference count
        Method *m_lastMethodDecRC;
        // Method where the refcount went to 0, if ever. If null, then
        // either RC never went to 0, or we don't have the method, depending
        // on the m_decToZero flag.
        Method *m_methodRCtoZero;
        // TODO: Is DeathSite _ALWAYS_ the same as RCtoZero method?
        //
        // Was this object's refcount ever decremented to zero?
        //     indeterminate - no refcount action
        //     false - last incremented to positive
        //     true - decremented to zero
        tribool m_decToZero;
        // Was this object incremented to positive AFTER being
        // decremented to zero?
        //     indeterminate - not yet decremented to zero
        //     false - decremented to zero
        //     true - decremented to zero, then incremented to positive
        tribool m_incFromZero;
        // METHOD 2: Use Elephant Track events instead
        LastEvent m_last_event;
        Object *m_last_object;

    public:
        Object( unsigned int id, unsigned int size,
                char kind, char* type,
                AllocSite* site, unsigned int els,
                Thread* thread, unsigned int create_time,
                HeapState* heap)
            : m_id(id)
            , m_size(size)
            , m_kind(kind)
            , m_type(type)
            , m_site(site)
            , m_elements(els)
            , m_thread(thread)
            , m_createTime(create_time)
            , m_deathTime(UINT_MAX)
            , m_refCount(0)
            , m_maxRefCount(0)
            , m_color(GREEN)
            , m_heapptr(heap)
            , m_pointed_by_heap(false)
            , m_was_root(false)
            , m_diedByHeap(false)
            , m_diedByStack(false)
            , m_reason(UNKNOWN_REASON)
            , m_last_action_time(0)
            , m_last_update_null(indeterminate)
            , m_methodDeathSite(0)
            , m_methodRCtoZero(NULL)
            , m_lastMethodDecRC(NULL)
            , m_decToZero(indeterminate)
            , m_incFromZero(indeterminate)
            , m_last_event(LastEvent::UNKNOWN_EVENT)
            , m_last_object(NULL) {
        }

        // -- Getters
        unsigned int getId() const { return m_id; }
        unsigned int getSize() const { return m_size; }
        const string& getType() const { return m_type; }
        char getKind() const { return m_kind; }
        Thread* getThread() const { return m_thread; }
        unsigned int getCreateTime() const { return m_createTime; }
        unsigned int getDeathTime() const { return m_deathTime; }
        Color getColor() const { return m_color; }
        EdgeMap::iterator const getEdgeMapBegin() { return m_fields.begin(); }
        EdgeMap::iterator const getEdgeMapEnd() { return m_fields.end(); }

        bool wasPointedAtByHeap() const { return m_pointed_by_heap; }
        void setPointedAtByHeap() { m_pointed_by_heap = true; }
        bool wasRoot() const { return m_was_root; }
        void setRootFlag( unsigned int t ) {
            m_was_root = true;
            m_reason = STACK;
            m_last_action_time = t;
        }
        bool getDiedByStackFlag() const { return m_diedByStack; }
        void setDiedByStackFlag() { m_diedByStack = true; m_reason = STACK; }
        void setStackReason( unsigned int t ) { m_reason = STACK; m_last_action_time = t; }
        bool getDiedByHeapFlag() const { return m_diedByHeap; }
        void setDiedByHeapFlag() { m_diedByHeap = true; m_reason = HEAP; }
        void setHeapReason( unsigned int t ) { m_reason = HEAP; m_last_action_time = t; }
        Reason setReason( Reason r, unsigned int t ) { m_reason = r; m_last_action_time = t; }
        Reason getReason() const { return m_reason; }
        unsigned int getLastActionTime() const { return m_last_action_time; }
        // Returns whether last update to this object was NULL.
        // If indeterminate, then there have been no updates
        tribool wasLastUpdateNull() const { return m_last_update_null; }
        // Set the last update null flag to true
        void setLastUpdateNull() { m_last_update_null = true; }
        // Set the last update null flag to false
        void unsetLastUpdateNull() { m_last_update_null = false; }
        // Get the death site according the the Death event
        Method *getDeathSite() const { return m_methodDeathSite; }
        // Set the death site because of a Death event
        void setDeathSite(Method * method) { m_methodDeathSite = method; }
        // Get the last method to decrement the reference count
        Method *getLastMethodDecRC() const { return m_lastMethodDecRC; }
        // Get the method to decrement the reference count to zero
        // -- If the refcount is ever incremented from zero, this is set back
        //    to NULL
        Method *getMethodDecToZero() const { return m_methodRCtoZero; }
        // No corresponding set of lastMethodDecRC because set happens through
        // decrementRefCountReal
        tribool wasDecrementedToZero() { return m_decToZero; }
        tribool wasIncrementedFromZero() const { return m_incFromZero; }
        // Set and get last event
        void setLastEvent( LastEvent le ) { m_last_event = le; }
        LastEvent getLastEvent() const { return m_last_event; }
        // Set and get last Object 
        void setLastObject( Object *obj ) { m_last_object = obj; }
        Object * getLastObject() const { return m_last_object; }

        // -- Ref counting
        unsigned int getRefCount() const { return m_refCount; }
        unsigned int getMaxRefCount() const { return m_maxRefCount; }
        void incrementRefCount() { m_refCount++; }
        void decrementRefCount() { m_refCount--; }
        void incrementRefCountReal();
        void decrementRefCountReal(unsigned int cur_time, Method *method, Reason r);
        // -- Access the fields
        const EdgeMap& getFields() const { return m_fields; }
        // -- Get a string representation
        string info();
        // -- Get a string representation for a dead object
        string info2();
        // -- Check live
        bool isLive(unsigned int tm) const { return (tm < m_deathTime); }
        // -- Update a field
        void updateField( Edge* edge,
                          unsigned int fieldId,
                          unsigned int cur_time,
                          Method *method,
                          Reason reason );
        // -- Record death time
        void makeDead(unsigned int death_time);
        // -- Set the color
        void recolor(Color newColor);
        // Mark object as red
        void mark_red();
        // Searches for a GREEN object
        void scan();
        // Recolors all nodes visited GREEN.
        void scan_green();
        // Searches for garbage cycle
        deque<int> collect_blue( deque< pair<int,int> >& edgelist );

        // Global debug counter
        static unsigned int g_counter;
};

class Edge
{
    private:
        // -- Source object
        Object* m_source;
        // -- Source field
        unsigned int m_sourceField;
        // -- Target object
        Object* m_target;
        // -- Creation time
        unsigned int m_createTime;
        // -- End time
        //    If 0 then ends when source object dies
        unsigned int m_endTime;

    public:
        Edge( Object* source, unsigned int field_id,
              Object* target, unsigned int cur_time )
            : m_source(source)
            , m_sourceField(field_id)
            , m_target(target)
            , m_createTime(cur_time)
            , m_endTime(0) {
        }

        Object* getSource() const { return m_source; }
        Object* getTarget() const { return m_target; }
        unsigned int getSourceField() const { return m_sourceField; }
        unsigned int getCreateTime() const { return m_createTime; }
        unsigned int getEndTime() const { return m_endTime; }

        void setEndTime(unsigned int end) { m_endTime = end; }
};

#endif
