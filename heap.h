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
#include <boost/bimap.hpp>

#include "classinfo.h"
#include "refstate.h"

class Object;
class Thread;
class Edge;

enum class Reason
    : std::uint8_t {
    STACK,
    HEAP,
    GLOBAL,
    END_OF_PROGRAM_REASON,
    UNKNOWN_REASON
};

enum class LastEvent
    : std::uint16_t {
    NEWOBJECT,
    ROOT,
    DECRC,
    UPDATE_UNKNOWN,
    UPDATE_AWAY_TO_NULL,
    UPDATE_AWAY_TO_VALID,
    OBJECT_DEATH_AFTER_ROOT,
    OBJECT_DEATH_AFTER_UPDATE,
    OBJECT_DEATH_AFTER_ROOT_DECRC, // from OBJECT_DEATH_AFTER_ROOT
    OBJECT_DEATH_AFTER_UPDATE_DECRC, // from OBJECT_DEATH_AFTER_UPDATE
    OBJECT_DEATH_AFTER_UNKNOWN,
    END_OF_PROGRAM_EVENT,
    UNKNOWN_EVENT,
};

string lastevent2str( LastEvent le );
bool is_object_death( LastEvent le );

enum DecRCReason {
    UPDATE_DEC = 2,
    DEC_TO_ZERO = 7,
    END_OF_PROGRAM_DEC = 8,
    UNKNOWN_DEC_EVENT = 99,
};

enum class KeyType {
    DAG = 1,
    DAGKEY = 2,
    CYCLE = 3,
    CYCLEKEY = 4,
    UNKNOWN_KEYTYPE = 99,
};

string keytype2str( KeyType ktype );

enum class CPairType
    : std::uint8_t {
    CP_Call,
    CP_Return,
    CP_Both,
    CP_None
};


typedef unsigned int ObjectId_t;
typedef unsigned int FieldId_t;
typedef unsigned int VTime_t;
typedef map<ObjectId_t, Object *> ObjectMap;
typedef map<ObjectId_t, Edge *> EdgeMap;
typedef set<Object *> ObjectSet;
typedef set<Edge *> EdgeSet;
typedef deque< pair<int, int> > EdgeList;
typedef std::map< Object *, std::set< Object * > * > KeySet_t;

enum class EdgeState : std::uint8_t; // forward declaration
// typedef map< Edge *, EdgeState > EdgeStateMap;
typedef map< std::pair<Edge *, VTime_t>, EdgeState > EdgeStateMap;
typedef set< std::pair<Edge *, VTime_t>, EdgeState > EdgeStateSet;

typedef map<Method *, set<string> *> DeathSitesMap;
// Where we save the method death sites. This has to be method pointer
// to:
// 1) set of object pointers
// 2) set of object types
// 2 seems better.
using namespace boost;
using namespace boost::logic;

typedef std::pair<int, int> GEdge_t;
typedef unsigned int  NodeId_t;
typedef std::map<int, int> Graph_t;
typedef std::map<Object *, Object *> ObjectPtrMap_t;
// TODO do we need to distinguish between FOUND and LOOKING?
// BiMap_t is used to map:
//     key object <-> some object
// where:
//     key objects map to themselves
//     non key objects to their root key objects
typedef bimap<ObjectId_t, ObjectId_t> BiMap_t;

// enum KeyStatus {
//     LOOKING = 1,
//     FOUND = 2,
//     UNKNOWN_STATUS = 99,
// };
// typedef map<ObjectId_t, std::pair<KeyStatus, ObjectId_t>> LookupMap;

struct compclass {
    bool operator() ( const std::pair< ObjectId_t, unsigned int >& lhs,
                      const std::pair< ObjectId_t, unsigned int >& rhs ) const {
        return lhs.second > rhs.second;
    }
};

class HeapState {
    friend class Object;
    public:

        // -- Do ref counting?
        static bool do_refcounting;

        // -- Turn on debugging
        static bool debug;

        // -- Turn on output of objects to stdout
        bool m_obj_debug_flag;

    protected:
        void save_output_edge( Edge *edge,
                               EdgeState estate );

    private:
        // -- Map from IDs to objects
        ObjectMap m_objects;
        // Live set
        ObjectSet m_liveset;

        // -- Set of edges (all pointers)
        EdgeSet m_edges;

        // Map from IDs to bool if possible cyle root
        map<unsigned int, bool> m_candidate_map;

        // Map from Edge * to Edgestate. This is where we save Edges which 
        // we can't output to the edgeinfo file yet because we don't know the
        // edge death time
        EdgeStateMap m_estate_map;

        unsigned long int m_liveSize; // current live size of program in bytes
        unsigned long int m_maxLiveSize; // max live size of program in bytes
        unsigned int m_alloc_time; // current alloc time

        // Total number of objects that died by loss of heap reference version 2
        unsigned int m_totalDiedByHeap_ver2;
        // Total number of objects that died by stack frame going out of scope version 2
        unsigned int m_totalDiedByStack_ver2;
        // Total number of objects that died by loss of global/static reference
        unsigned int m_totalDiedByGlobal;
        // Total number of objects that live till the end of the program
        unsigned int m_totalDiedAtEnd;
        // Total number of objects unknown using version 2 method
        unsigned int m_totalDiedUnknown_ver2;
        // Size of objects that died by loss of heap reference
        unsigned int m_sizeDiedByHeap;
        // Size of objects that died by loss of global heap reference
        unsigned int m_sizeDiedByGlobal;
        // Size of objects that died by stack frame going out of scope
        unsigned int m_sizeDiedByStack;
        // Size of objects that live till the end of the program
        unsigned int m_sizeDiedAtEnd;

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
        
        // Map of object to key object (in pointers)
        ObjectPtrMap_t& m_whereis;

        // Map of key object to set of objects
        KeySet_t& m_keyset;

        void update_death_counters( Object *obj );
        Method * get_method_death_site( Object *obj );

        NodeId_t getNodeId( ObjectId_t objId, bimap< ObjectId_t, NodeId_t >& bmap );

    public:
        HeapState( ObjectPtrMap_t& whereis, KeySet_t& keyset )
            : m_objects()
            , m_liveset()
            , m_candidate_map()
            , m_edges()
            , m_estate_map()
            , m_death_sites_map()
            , m_whereis( whereis )
            , m_keyset( keyset )
            , m_maxLiveSize(0)
            , m_liveSize(0)
            , m_alloc_time(0)
            , m_totalDiedByHeap_ver2(0)
            , m_totalDiedByStack_ver2(0)
            , m_totalDiedByGlobal(0)
            , m_totalDiedAtEnd(0)
            , m_sizeDiedByHeap(0)
            , m_sizeDiedByGlobal(0)
            , m_sizeDiedByStack(0)
            , m_sizeDiedAtEnd(0)
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
                          char *type,
                          AllocSite *site, 
                          unsigned int els,
                          Thread *thread,
                          unsigned int create_time );

        Object* getObject(unsigned int id);

        Edge* make_edge( Object* source, unsigned int field_id, Object* target, unsigned int cur_time);

        void makeDead( Object * obj,
                       unsigned int death_time,
                       ofstream &eifile );

        ObjectMap::iterator begin() { return m_objects.begin(); }
        ObjectMap::iterator end() { return m_objects.end(); }
        unsigned int size() const { return m_objects.size(); }
        unsigned long int liveSize() const { return m_liveSize; }
        unsigned long int maxLiveSize() const { return m_maxLiveSize; }
        unsigned int getAllocTime() const { return m_alloc_time; }

        unsigned int getTotalDiedByStack2() const { return m_totalDiedByStack_ver2; }
        unsigned int getTotalDiedByHeap2() const { return m_totalDiedByHeap_ver2; }
        unsigned int getTotalDiedByGlobal() const { return m_totalDiedByGlobal; }
        unsigned int getTotalDiedAtEnd() const { return m_totalDiedAtEnd; }
        unsigned int getTotalDiedUnknown() const { return m_totalDiedUnknown_ver2; }
        unsigned int getSizeDiedByHeap() const { return m_sizeDiedByHeap; }
        unsigned int getSizeDiedByStack() const { return m_sizeDiedByStack; }
        unsigned int getSizeDiedAtEnd() const { return m_sizeDiedAtEnd; }

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

        void addEdge(Edge* e) { m_edges.insert(e); }
        EdgeSet::iterator begin_edges() { return m_edges.begin(); }
        EdgeSet::iterator end_edges() { return m_edges.end(); }
        unsigned int numberEdges() { return m_edges.size(); }

        void end_of_program( unsigned int cur_time,
                             ofstream &edge_info_file );

        void set_candidate(unsigned int objId);
        void unset_candidate(unsigned int objId);
        deque< deque<int> > scan_queue( EdgeList& edgelist );
        void scan_queue2( EdgeList& edgelist,
                          map<unsigned int, bool>& ncmap );
        void set_reason_for_cycles( deque< deque<int> >& cycles );

        ObjectPtrMap_t& get_whereis() { return m_whereis; }
        KeySet_t& get_keyset() { return m_keyset; }

        // EdgeState map and set related functions
        EdgeStateMap::iterator begin_edgestate_map() {
            return this->m_estate_map.begin();
        }
        EdgeStateMap::iterator end_edgestate_map() {
            return this->m_estate_map.end();
        }
};

enum class Color
    : std::uint8_t {
    BLUE = 1,
    RED = 2,
    PURPLE = 3, // UNUSED
    BLACK = 4,
    GREEN = 5,
};

enum class ObjectRefType
    : std::uint8_t {
    SINGLY_OWNED, // Only one incoming reference ever which 
                      // makes the reference RefType::SERIAL_STABLE
    MULTI_OWNED, // Gets passed around to difference edge sources
    UNKNOWN
};

enum class EdgeState 
    : std::uint8_t {
    NONE = 1,
    LIVE = 2,
    DEAD_BY_UPDATE = 3,
    DEAD_BY_OBJECT_DEATH = 4,
    DEAD_BY_PROGRAM_END = 5,
    DEAD_BY_OBJECT_DEATH_NOT_SAVED = 6,
    DEAD_BY_PROGRAM_END_NOT_SAVED = 7,
    // The not SAVED versions means that we haven't written the
    // edge out to the edgeinfo file yet.
};


class Object {
    private:
        unsigned int m_id;
        unsigned int m_size;
        char m_kind;
        string m_type;
        AllocSite *m_site;
        string m_allocsite_name;
        unsigned int m_elements;
        Thread *m_thread;

        unsigned int m_createTime;
        unsigned int m_deathTime;
        unsigned int m_createTime_alloc;
        unsigned int m_deathTime_alloc;

        unsigned int m_refCount;
        Color m_color;
        ObjectRefState m_refState;
        unsigned int m_maxRefCount;

        EdgeMap m_fields;

        HeapState *m_heapptr;
        bool m_deadFlag;

        // Was this object ever a target of a heap pointer?
        bool m_pointed_by_heap;
        // Was this object ever a root?
        bool m_was_root;
        // Did last update move to NULL?
        tribool m_last_update_null; // If false, it moved to a differnet object
        // Was last update away from this object from a static field?
        bool m_last_update_away_from_static;
        // Did this object die by loss of heap reference?
        bool m_diedByHeap;
        // Did this object die by loss of stack reference?
        bool m_diedByStack;
        // Did this object die because the program ended?
        bool m_diedAtEnd;
        // Did this object die because of an update away from a global/static variable?
        bool m_diedByGlobal;
        // Has the diedBy***** flag been set?
        bool m_diedFlagSet;
        // Reason for death
        Reason m_reason;
        // Time that m_reason happened
        unsigned int m_last_action_time;
        // Time of last update away. This is the 'timestamp' in the Merlin algorithm
        unsigned int m_last_timestamp;
        // Time of last update away. This is the 'timestamp' in the Merlin algorithm
        unsigned int m_actual_last_timestamp;
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

        // TODO: // Simple (ContextPair) context of where this object died. Type is defined in classinfo.h
        // TODO: // And the associated type.
        // TODO: ContextPair m_death_cpair;
        // TODO: CPairType  m_death_cptype;
        // TODO: // Simple (ContextPair) context of where this object was allocated. Type is defined in classinfo.h
        // TODO: // And the associated type.
        // TODO: ContextPair m_alloc_cpair;
        // TODO: CPairType  m_alloc_cptype;
        // TODO: // NOTE: This could have been made into a single class which felt like overkill.
        // TODO: // The option is there if it seems better to do so, but chose to go the simpler route.

        string m_deathsite_name;

        DequeId_t m_alloc_context;
        // If ExecMode is Full, this contains the full list of the stack trace at allocation.
        DequeId_t m_death_context;
        // If ExecMode is Full, this contains the full list of the stack trace at death.

        // Who's my key object? 0 means unassigned.
        Object *m_death_root;
        KeyType m_key_type;

        // Stability type
        ObjectRefType m_reftarget_type;

    public:
        Object( unsigned int id,
                unsigned int size,
                char kind,
                char* type,
                AllocSite* site,
                unsigned int els,
                Thread* thread,
                unsigned int create_time,
                HeapState* heap )
            : m_id(id)
            , m_size(size)
            , m_kind(kind)
            , m_type(type)
            , m_site(site)
            , m_elements(els)
            , m_thread(thread)
            , m_deadFlag(false)
            , m_createTime(create_time)
            , m_deathTime(UINT_MAX)
            , m_createTime_alloc( heap->getAllocTime() )
            , m_deathTime_alloc(UINT_MAX)
            , m_refCount(0)
            , m_maxRefCount(0)
            , m_color(Color::GREEN)
            , m_heapptr(heap)
            , m_pointed_by_heap(false)
            , m_was_root(false)
            , m_diedByHeap(false)
            , m_diedByStack(false)
            , m_diedAtEnd(false)
            , m_diedByGlobal(false)
            , m_diedFlagSet(false)
            , m_reason(Reason::UNKNOWN_REASON)
            , m_last_action_time(0)
            , m_last_timestamp(0)
            , m_actual_last_timestamp(0)
            , m_last_update_null(indeterminate)
            , m_last_update_away_from_static(false)
            , m_methodDeathSite(0)
            , m_methodRCtoZero(NULL)
            , m_lastMethodDecRC(NULL)
            , m_decToZero(indeterminate)
            , m_incFromZero(indeterminate)
            , m_last_event(LastEvent::UNKNOWN_EVENT)
            , m_death_root(NULL)
            , m_last_object(NULL)
            , m_key_type(KeyType::UNKNOWN_KEYTYPE)
            , m_deathsite_name("NONE")
            // , m_death_cpair(NULL, NULL)
            , m_reftarget_type(ObjectRefType::UNKNOWN)
        {
            if (m_site) {
                Method *mymeth = m_site->getMethod();
                m_allocsite_name = (mymeth ? mymeth->getName() : "NONAME");
            } else {
                m_allocsite_name = "NONAME";
            }
        }

        // -- Getters
        unsigned int getId() const { return m_id; }
        unsigned int getSize() const { return m_size; }
        const string& getType() const { return m_type; }
        char getKind() const { return m_kind; }
        AllocSite * getAllocSite() const { return m_site; }
        string getAllocSiteName() const { return m_allocsite_name; }
        Thread * getThread() const { return m_thread; }
        VTime_t getCreateTime() const { return m_createTime; }
        VTime_t getDeathTime() const {
            return m_deathTime;
        }
        void setDeathTime( VTime_t new_deathtime );

        VTime_t getCreateTimeAlloc() const { return this->m_createTime_alloc; }
        VTime_t getDeathTimeAlloc() const { return m_deathTime_alloc; }
        Color getColor() const { return m_color; }
        EdgeMap::iterator const getEdgeMapBegin() { return m_fields.begin(); }
        EdgeMap::iterator const getEdgeMapEnd() { return m_fields.end(); }
        bool isDead() const { return m_deadFlag; }

        bool wasPointedAtByHeap() const { return m_pointed_by_heap; }
        void setPointedAtByHeap() { m_pointed_by_heap = true; }
        bool wasRoot() const { return m_was_root; }
        void setRootFlag( unsigned int t ) {
            m_was_root = true;
            m_reason = Reason::STACK;
            m_last_action_time = t;
        }

        // ==================================================
        // The diedBy***** flags
        // - died by STACK
        bool getDiedByStackFlag() const { return m_diedByStack; }
        void setDiedByStackFlag() {
            // REMOVE this DEBUG for now
            // TODO if (this->m_diedFlagSet) {
            // TODO     // Check to see if different
            // TODO     if ( this->m_diedByHeap || this->m_diedByGlobal) {
            // TODO         cerr << "Object[" << this->m_id << "]"
            // TODO              << " was originally died by heap. Overriding." << endl;
            // TODO     }
            // TODO }
            this->m_diedByStack = true;
            this->m_reason = Reason::STACK;
            this->m_diedFlagSet = true;
        }
        void unsetDiedByStackFlag() { m_diedByStack = false; }
        void setStackReason( unsigned int t ) { m_reason = Reason::STACK; m_last_action_time = t; }
        // -----------------------------------------------------------------
        // - died by HEAP 
        bool getDiedByHeapFlag() const { return m_diedByHeap; }
        void setDiedByHeapFlag() {
            // REMOVE this DEBUG for now
            // TODO if (this->m_diedFlagSet) {
            // TODO     // Check to see if different
            // TODO     if (this->m_diedByStack) {
            // TODO         cerr << "Object[" << this->m_id << "]"
            // TODO              << " was originally died by stack. Overriding." << endl;
            // TODO     }
            // TODO }
            this->m_diedByHeap = true;
            this->m_reason = Reason::HEAP;
            this->m_diedFlagSet = true;
        }
        void unsetDiedByHeapFlag() { m_diedByHeap = false; }
        // -----------------------------------------------------------------
        // - died by GLOBAL
        bool getDiedByGlobalFlag() const { return m_diedByGlobal; }
        void setDiedByGlobalFlag() {
            // REMOVE this DEBUG for now
            // TODO if (this->m_diedFlagSet) {
            // TODO     // Check to see if different
            // TODO     if ( this->m_diedByHeap ) {
            // TODO         cerr << "Object[" << this->m_id << "]"
            // TODO              << " was originally died by heap but trying to set diedByGlobal. Overriding."
            // TODO              << endl;
            // TODO     } else if (this->m_diedByStack) {
            // TODO         cerr << "Object[" << this->m_id << "]"
            // TODO              << " was originally died by stack but setting to by GLOBAL. Overriding."
            // TODO              << endl;
            // TODO     }
            // TODO }
            this->m_diedByGlobal = true;
            this->m_reason = Reason::GLOBAL;
            this->m_diedFlagSet = true;
        }
        void unsetDiedByGlobalFlag() { m_diedByGlobal = false; }
        // -----------------------------------------------------------------
        // - died at END
        bool getDiedAtEndFlag() const { return m_diedAtEnd; }
        void setDiedAtEndFlag() {
            if (this->m_diedFlagSet) {
                cerr << "Object[" << this->m_id << "]"
                     << " was has died by " << this->flagName()
                     <<  "flag set. NOT overriding." << endl;
                return;
            }
            this->m_diedAtEnd = true;
            this->m_reason = Reason::END_OF_PROGRAM_REASON;
            this->m_diedFlagSet = true;
        }
        void unsetDiedAtEndFlag() { m_diedAtEnd = false; }
        bool isDiedFlagSet() { return this->m_diedFlagSet; }
        // -----------------------------------------------------------------
        string flagName() {
            return ( this->getDiedByHeapFlag() ? "HEAP" : 
                     ( this->getDiedByStackFlag() ? "STACK" :
                       ( this->getDiedAtEndFlag() ? "END" :
                         "OTHER" ) ) );
        }
        // -----------------------------------------------------------------

        void setHeapReason( unsigned int t ) { m_reason = Reason::HEAP; m_last_action_time = t; }
        Reason setReason( Reason r, unsigned int t ) { m_reason = r; m_last_action_time = t; }
        Reason getReason() const { return m_reason; }
        unsigned int getLastActionTime() const { return m_last_action_time; }
        // Return the Merlin timestamp
        auto getLastTimestamp() -> unsigned int const {
            return this->m_last_timestamp;
        }
        // Set the Merlin timestamp
        void setLastTimestamp( unsigned int new_ts ) {
            this->m_last_timestamp = new_ts;
        }
        // Return the actual non-Merlinized timestamp
        auto getActualLastTimestamp() -> unsigned int const {
            return this->m_actual_last_timestamp;
        }
        // Set the actual non-Merlinized timestamp
        void setActualLastTimestamp( unsigned int new_ts ) {
            this->m_actual_last_timestamp = new_ts;
        }

        // Returns whether last update to this object was NULL.
        // If indeterminate, then there have been no updates
        tribool wasLastUpdateNull() const { return m_last_update_null; }
        // Set the last update null flag to true
        void setLastUpdateNull() { m_last_update_null = true; }
        // Set the last update null flag to false
        void unsetLastUpdateNull() { m_last_update_null = false; }
        // Check if last update was from static
        bool wasLastUpdateFromStatic() const { return m_last_update_away_from_static; }
        // Set the last update from static flag to true
        void setLastUpdateFromStatic() { m_last_update_away_from_static = true; }
        // Set the last update from static flag to false
        void unsetLastUpdateFromStatic() { m_last_update_away_from_static = false; }
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
        // Set and get death root
        void setDeathRoot( Object *newroot ) { this->m_death_root = newroot; }
        Object * getDeathRoot() const { return this->m_death_root; }
        // Set and get key type 
        void setKeyType( KeyType newtype ) { this->m_key_type = newtype; }
        void setKeyTypeIfNotKnown( KeyType newtype ) {
            if (this->m_key_type == KeyType::UNKNOWN_KEYTYPE) {
                this->m_key_type = newtype;
            }
            // else {
            //     // TODO: Log some debugging.
            //     cerr << "Object[ " << this->m_id
            //          << "] keytype prev[ " << keytype2str(this->m_key_type)
            //          << "] new [ " << keytype2str(newtype) << "]"
            //          << endl;
            // }
        }
        KeyType getKeyType() const { return this->m_key_type; }

        // Set and get stability taret types
        void setRefTargetType( ObjectRefType newtype ) { this->m_reftarget_type = newtype; }
        ObjectRefType getRefTargetType() const { return this->m_reftarget_type; }

        // --------------------------------------------------------------------------------
        // ----[ Context pair related functions ]------------------------------------------
        // Get Allocation context pair. Note that if <NULL, NULL> then none yet assigned.
        // TODO ContextPair getAllocContextPair() const { return this->m_alloc_cpair; }
        // TODO // Set Allocation context pair. Note that if <NULL, NULL> then none yet assigned.
        // TODO ContextPair setAllocContextPair( ContextPair cpair, CPairType cptype ) {
        // TODO     this->m_alloc_cpair = cpair;
        // TODO     this->m_alloc_cptype = cptype;
        // TODO     return this->m_alloc_cpair;
        // TODO }
        // TODO // Get Allocation context type
        // TODO CPairType getAllocContextType() const { return this->m_alloc_cptype; }

        // TODO // Get Death context pair. Note that if <NULL, NULL> then none yet assigned.
        // TODO ContextPair getDeathContextPair() const { return this->m_death_cpair; }
        // TODO // Set Death context pair. Note that if <NULL, NULL> then none yet assigned.
        // TODO ContextPair setDeathContextPair( ContextPair cpair, CPairType cptype ) {
        // TODO     this->m_death_cpair = cpair;
        // TODO     this->m_death_cptype = cptype;
        // TODO     return this->m_death_cpair;
        // TODO }
        // TODO // Get Death context type
        // TODO CPairType getDeathContextType() const { return this->m_death_cptype; }
        // --------------------------------------------------------------------------------
        // --------------------------------------------------------------------------------

        // Single death context
        // Getter
        string getDeathContextSiteName() const {
            return this->m_deathsite_name;
        }
        // Setter
        void setDeathContextSiteName( string &new_dsite ) {
            this->m_deathsite_name = new_dsite;
        }

        // Set allocation context list
        void setAllocContextList( DequeId_t acontext_list ) {
            this->m_alloc_context = acontext_list;
        }
        // Get allocation context type
        DequeId_t getAllocContextList() const {
            return this->m_alloc_context;
        }

        // Set death context list
        void setDeathContextList( DequeId_t dcontext_list ) {
            this->m_alloc_context = dcontext_list;
        }

        // Get death context type
        DequeId_t getDeathContextList() const {
            return this->m_death_context;
        }


        // -- Ref counting
        unsigned int getRefCount() const { return m_refCount; }
        unsigned int getMaxRefCount() const { return m_maxRefCount; }
        void incrementRefCount() { m_refCount++; }
        void decrementRefCount() { m_refCount--; }
        void incrementRefCountReal();
        void decrementRefCountReal( unsigned int cur_time,
                                    Method *method,
                                    Reason r,
                                    Object *death_root,
                                    LastEvent last_event,
                                    ofstream &eifile );
        // -- Access the fields
        const EdgeMap& getFields() const { return m_fields; }
        // -- Get a string representation
        string info();
        // -- Get a string representation for a dead object
        string info2();
        // -- Check live
        bool isLive(unsigned int tm) const {
            return (this->m_deathTime >= tm);
        }
        // -- Update a field
        void updateField( Edge* edge,
                          FieldId_t fieldId,
                          unsigned int cur_time,
                          Method *method,
                          Reason reason,
                          Object *death_root,
                          LastEvent last_event,
                          ofstream &eifile );
        // -- Record death time
        void makeDead( unsigned int death_time,
                       unsigned int death_time_alloc,
                       EdgeState estate,
                       ofstream &eifile );
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

class Edge {
    private:
        // -- Source object
        Object *m_source;
        // -- Source field
        unsigned int m_sourceField;
        // -- Target object
        Object *m_target;
        // -- Creation time
        unsigned int m_createTime;
        // -- End time
        //    If 0 then ends when source object dies
        unsigned int m_endTime;
        // Died with source? (tribool state)
        // MAYBE == Unknown
        tribool m_died_with_source;
        // EdgeState
        EdgeState m_edgestate;
        // Flag on whether edge has been outputed to edgeinfo file
        bool m_output_done;

    public:
        Edge( Object *source, unsigned int field_id,
              Object *target, unsigned int cur_time )
            : m_source(source)
            , m_sourceField(field_id)
            , m_target(target)
            , m_createTime(cur_time)
            , m_endTime(0)
            , m_died_with_source(indeterminate)
            , m_edgestate(EdgeState::NONE)
            , m_output_done(false) {
        }

        Object *getSource() const {
            return m_source;
        }

        Object *getTarget() const {
            return m_target;
        }

        FieldId_t getSourceField() const {
            return m_sourceField;
        }

        unsigned int getCreateTime() const {
            return m_createTime;
        }

        unsigned int getEndTime() const {
            return m_endTime;
        }

        void setEndTime(unsigned int end) {
            m_endTime = end;
        }

        // EdgeState setter/getter
        EdgeState getEdgeState() const {
            return m_edgestate;
        }
        void setEdgeState(EdgeState newestate ) {
            // DEBUG
            // if (newestate == EdgeState::DEAD_BY_OBJECT_DEATH) {
            //     cerr << "X: DBOD" << endl; 
            // } else if (newestate == EdgeState::DEAD_BY_PROGRAM_END) {
            //     if (this->m_edgestate == EdgeState::DEAD_BY_OBJECT_DEATH) {
            //         cerr << "X: DBOD -> PROGEND" << endl;
            //     } else if (this->m_edgestate == EdgeState::LIVE) {
            //         cerr << "Y: LIVE -> PROGEND" << endl;
            //     } else if (this->m_edgestate == EdgeState::DEAD_BY_UPDATE) {
            //         cerr << "Z: DBU -> PROGEND" << endl;
            //     }
            // }
            this->m_edgestate = newestate;
        }

        // Flag on whether edge has been sent to edgeinfo output file
        //   setter/getter
        void setOutputFlag( bool flag ) {
            this->m_output_done = flag;
        }
        bool getOutputFlag() const {
            return this->m_output_done;
        }
};


#endif
