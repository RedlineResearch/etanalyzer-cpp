#ifndef EXECUTION_H
#define EXECUTION_H

// ----------------------------------------------------------------------
//  Representation of runtime execution
//  stacks, threads, time

#include <iostream>
#include <map>
#include <deque>
#include <utility>

#include "classinfo.h"
#include "heap.h"

using namespace std;

// Type definitions
typedef unsigned int MethodId_t;
// TODO typedef unsigned int threadId_t;

class CCNode;
typedef map<unsigned int, CCNode *> CCMap;
typedef map<Method *, CCNode *> FullCCMap;

typedef map<MethodId_t, Thread *> ThreadMap;
typedef map<ContextPair, unsigned int> ContextCountMap;
typedef map<Object *, ContextPair> ObjectContextMap;


typedef deque<Method *> MethodDeque;
typedef deque<Thread *> ThreadDeque;
typedef set<Object *> LocalVarSet;
typedef deque<LocalVarSet *> LocalVarDeque;

// TODO typedef deque< pair<LastEvent, Object*> > LastEventDeque_t;
// TODO typedef map<threadId_t, LastEventDeque_t> LastMap_t;

// ----------------------------------------------------------------------
//   Calling context tree

enum class ExecMode {
    CCMode = 1,
    MethodMode = 2
};

enum class EventKind {
    Allocation = 1,
    Death = 2,
};

class CCNode
{
    private:
        Method* m_method;
        CCNode* m_parent;
        // -- Map from method IDs to callee contexts
        CCMap m_callees;
        // Flag indicating whether simple method trace has been saved
        bool m_done;
        // Caching the simple_stacktrace
        deque<Method *> m_simptrace;

        unsigned int m_node_id;
        static unsigned int m_ccnode_nextid;

        unsigned int get_next_node_id() {
            unsigned int result = this->m_ccnode_nextid;
            this->m_ccnode_nextid++;
            return result;
        }

        // File to output the callstack
        ofstream &m_output;

    public:
        CCNode( ofstream &output )
            : m_method(0)
            , m_parent(0)
            , m_done(false)
            , m_node_id(this->get_next_node_id())
            , m_output(output) {
        }

        CCNode( CCNode* parent, Method* m, ofstream &output )
            : m_method(m)
            , m_parent(parent)
            , m_node_id(this->get_next_node_id())
            , m_output(output) {
        }

        // -- Get method
        Method* getMethod() const { return m_method; }

        // -- Get parent context (if there is one)
        CCNode* getParent() const { return m_parent; }

        // -- Call a method, making a new child context if necessary
        CCNode* Call(Method* m);

        // -- Return from a method, returning the parent context
        CCNode* Return(Method* m);

        // -- Produce a string representation of the context
        string info();

        // -- Generate a stack trace
        string stacktrace();

        // Method name equality
        bool simple_cc_equal( CCNode &other );

        // TODO
        deque<Method *> simple_stacktrace();

        CCMap::iterator begin_callees() { return this->m_callees.begin(); }
        CCMap::iterator end_callees() { return this->m_callees.end(); }

        // Has simple trace been saved for this CCNode?
        bool isDone() { return this->m_done; }
        bool setDone() { this->m_done = true; }

        // Node Ids
        NodeId_t get_node_id() const { return this->m_node_id; }
};


// ----------------------------------------------------------------------
//   Thread representation

//  Two options for representing the stack
//    (1) Build a full calling context tree
//    (2) Keep a stack of methods
//  (I realize I could probably do this with fancy-dancy OO programming,
//  but sometimes that just seems like overkill

class ExecState; // forward declaration to include into Thread

class Thread
{
    private:
        // -- Thread ID
        unsigned int m_id;
        // -- Kind of stack
        unsigned int m_kind;
        // -- CC tree representation
        CCNode *m_curcc;
        // -- Stack of methods
        MethodDeque m_methods;
        // -- Local stack variables that have root events in this scope
        LocalVarDeque m_locals;
        // -- Local stack variables that have root events and died this scope
        LocalVarDeque m_deadlocals;
        // -- Current context pair
        ContextPair m_context;
        // -- Type of ContextPair m_context
        CPairType m_cptype;
        // -- Map of simple Allocation context pair -> count of occurrences
        ContextCountMap &m_allocCountmap;
        // -- Map of simple Death context pair -> count of occurrences
        ContextCountMap &m_deathCountmap;
        // -- Map to ExecState
        ExecState &m_exec;
        // File to output the callstack
        ofstream &m_output;
        // File to output the nodeId to method name
        ofstream &m_nodefile;

    public:
        Thread( unsigned int id,
                unsigned int kind,
                ContextCountMap &allocCountmap,
                ContextCountMap &deathCountmap,
                ExecState &execstate,
                ofstream &output,
                ofstream &nodefile )
            : m_id(id)
            , m_kind(kind)
            , m_rootcc(output)
            , m_curcc(&m_rootcc)
            , m_context( NULL, NULL )
            , m_cptype(CPairType::CP_None) 
            , m_allocCountmap(allocCountmap)
            , m_deathCountmap(deathCountmap)
            , m_exec(execstate)
            , m_output(output)
            , m_nodefile(nodefile) {
            m_locals.push_back(new LocalVarSet());
            m_deadlocals.push_back(new LocalVarSet());
        }

        unsigned int getId() const { return m_id; }

        // -- Call method m
        void Call(Method* m);
        // -- Return from method m
        void Return(Method* m);
        // -- Get current CC
        CCNode* TopCC();
        // -- Get current method
        Method* TopMethod();
        // -- Get current dead locals
        LocalVarSet * TopLocalVarSet();
        // -- Get a stack trace
        string stacktrace();
        // -- Root event
        void objectRoot(Object * object);
        // -- Check dead object
        bool isLocalVariable(Object *object);
        // Root CCNode
        CCNode m_rootcc;
        // Get root node CC
        CCNode &getRootCCNode() { return m_rootcc; }
        // Get simple context pair
        ContextPair getContextPair() const { return m_context; }
        // Set simple context pair
        ContextPair setContextPair( ContextPair cpair, CPairType cptype ) {
            this->m_context = cpair;
            this->m_cptype = cptype;
            return cpair; 
        }
        // Get simple context pair type
        CPairType getContextPairType() const { return this->m_cptype; }
        // Set simple context pair type
        void setContextPairType( CPairType cptype ) { this->m_cptype = cptype; }

        // Debug
        void debug_cpair( ContextPair cpair,
                          string ptype ) {
            Method *m1 = std::get<0>(cpair);
            Method *m2 = std::get<1>(cpair);
            string method1 = (m1 ? m1->getName() : "NONAME1");
            string method2 = (m2 ? m2->getName() : "NONAME2");
            cout << "CPAIR-dbg< " << ptype << " >" 
                 << "[ " << method1 << ", " << method2 << "]" << endl;
        }
};

// ----------------------------------------------------------------------
// ----------------------------------------------------------------------
//   Execution state
// ----------------------------------------------------------------------

class ExecState
{
    private:
        // -- Stack kind (CC or methods)
        unsigned int m_kind;
        // -- Set of threads
        ThreadMap m_threads;
        // -- Time
        unsigned int m_meth_time;
        // -- Update Time
        unsigned int m_uptime;
        // -- Alloc Time
        unsigned int m_alloc_time;
        // -- Map of Object pointer -> simple allocation context pair
        ObjectContextMap m_objAlloc2cmap;
        // -- Map of Object pointer -> simple death context pair
        ObjectContextMap m_objDeath2cmap;
        // Last method called
        ThreadDeque m_thread_stack;

    public:
        ExecState( unsigned int kind )
            : m_kind(kind)
            , m_threads()
            , m_meth_time(0)
            , m_uptime(0)
            , m_alloc_time(0)
            , m_allocCountmap()
            , m_deathCountmap()
            , m_objAlloc2cmap()
            , m_objDeath2cmap()
            , m_thread_stack()
            , m_output(NULL)
            , m_nodefile(NULL) {
        }

        // -- Get the current time
        unsigned int MethNow() const { return m_meth_time; }

        // -- Get the current update time
        unsigned int NowUp() const { return m_uptime + m_meth_time; }

        // -- Get the current allocation time
        unsigned int NowAlloc() const { return m_alloc_time; }
        // -- Set the current allocation time
        void SetAllocTime( unsigned int newtime ) { this->m_alloc_time = newtime; }

        // -- Set the current update time
        inline unsigned int SetUpdateTime( unsigned int newutime ) {
            return this->m_uptime = newutime;
        }

        // -- Increment the current update time
        inline unsigned int IncUpdateTime() {
            return this->m_uptime++;
        }

        // -- Look up or create a thread
        Thread* getThread(unsigned int threadid);

        // -- Call method m in thread t
        void Call(Method* m, unsigned int threadid);

        // -- Return from method m in thread t
        void Return(Method* m, unsigned int threadid);

        // -- Get the top method in thread t
        Method* TopMethod(unsigned int threadid);

        // -- Get the top calling context in thread t
        CCNode* TopCC(unsigned int threadid);

        // Get begin iterator of thread map
        ThreadMap::iterator begin_threadmap() { return this->m_threads.begin(); }
        ThreadMap::iterator end_threadmap() { return this->m_threads.end(); }

        // Update the Object pointer to simple Allocation context pair map
        void UpdateObj2AllocContext( Object *obj,
                                     ContextPair cpair,
                                     CPairType cptype ) {
            UpdateObj2Context( obj,
                               cpair,
                               cptype,
                               EventKind::Allocation );
        }

        // Update the Object pointer to simple Death context pair map
        void UpdateObj2DeathContext( Object *obj,
                                     ContextPair cpair,
                                     CPairType cptype ) {
            UpdateObj2Context( obj,
                               cpair,
                               cptype,
                               EventKind::Death );
        }

        // Update the Object pointer to simple context pair map
        void UpdateObj2Context( Object *obj,
                                ContextPair cpair,
                                CPairType cptype,
                                EventKind ekind ) {
            assert(obj);
            // DEBUG cpair here
            // TODO debug_cpair( obj->getDeathContextPair(), obj );
            // END DEBUG
            if (ekind == EventKind::Allocation) {
                this->m_objAlloc2cmap[obj] = cpair;
                obj->setAllocContextPair( cpair, cptype );
            } else {
                assert( ekind == EventKind::Death );
                this->m_objDeath2cmap[obj] = cpair;
                obj->setDeathContextPair( cpair, cptype );
            }

            ContextCountMap &curcmap = ((ekind == EventKind::Allocation) ? this->m_allocCountmap
                                                                         : this->m_deathCountmap);
            auto it = curcmap.find( cpair );
            if (it != curcmap.end()) {
                curcmap[cpair] += 1; 
            } else {
                curcmap[cpair] = 1; 
            }
        }

        // -- Map of simple Allocation context pair -> count of occurrences
        // TODO: Think about hiding this in an abstraction TODO
        ContextCountMap m_allocCountmap;
        ContextCountMap::iterator begin_allocCountmap() { return this->m_allocCountmap.begin(); }
        ContextCountMap::iterator end_allocCountmap() { return this->m_allocCountmap.end(); }

        // -- Map of simple Death context pair -> count of occurrences
        // TODO: Think about hiding this in an abstraction TODO
        ContextCountMap m_deathCountmap;
        ContextCountMap::iterator begin_deathCountmap() { return this->m_deathCountmap.begin(); }
        ContextCountMap::iterator end_deathCountmap() { return this->m_deathCountmap.end(); }

        // Get last global thread called
        Thread *get_last_thread() const {
            return ( (this->m_thread_stack.size() > 0)
                     ? this->m_thread_stack.back()
                     : NULL );
        }

        unsigned int get_kind() const { return m_kind; }

        // File to output the callstack
        ofstream *m_output;
        void set_output( ofstream *out ) { this->m_output = out; }
        // File to output the node id to method name map 
        ofstream *m_nodefile;
        void set_nodefile( ofstream *nfile ) { this->m_nodefile = nfile; }


    private:
        void debug_cpair( ContextPair cpair,
                          Object *object ) {
            Method *m1 = std::get<0>(cpair);
            Method *m2 = std::get<1>(cpair);
            string method1 = (m1 ? m1->getName() : "NONAME1");
            string method2 = (m2 ? m2->getName() : "NONAME2");
            cout << "CPAIR-update< " << object->getType() << " >"
                << "[ " << method1 << ", " << method2 << "]" << endl;
        }

};

#endif

