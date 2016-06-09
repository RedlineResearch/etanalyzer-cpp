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

typedef map<MethodId_t, Thread *> ThreadMap;
typedef map<ContextPair, unsigned int> ContextCountMap;
typedef map<Object *, ContextPair> ObjectContextMap;


typedef deque<Method *> MethodDeque;
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

    public:
        CCNode()
            : m_method(0)
            , m_parent(0)
            , m_done(false) {
        }

        CCNode( CCNode* parent, Method* m )
            : m_method(m)
            , m_parent(parent) {
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
        // -- Map of simple context pair -> count of occurrences
        ContextCountMap &m_ccountmap;
        // -- Map to ExecState
        ExecState &m_exec;

    public:
        Thread( unsigned int id,
                unsigned int kind,
                ContextCountMap &ccountmap,
                ExecState &execstate )
            : m_id(id)
            , m_kind(kind)
            , m_rootcc()
            , m_curcc(&m_rootcc)
            , m_context( NULL, NULL )
            , m_ccountmap( ccountmap )
            , m_exec(execstate) {
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
        // Get simple context pair
        ContextPair setContextPair( ContextPair cpair ) {
            this->m_context = cpair;
            return cpair; 
        }
};

// ----------------------------------------------------------------------
//   Execution state

class ExecState
{
    private:
        // -- Stack kind (CC or methods)
        unsigned int m_kind;
        // -- Set of threads
        ThreadMap m_threads;
        // -- Time
        unsigned int m_time;
        // -- Update Time
        unsigned int m_uptime;
        // -- Map of Object pointer -> simple context pair
        ObjectContextMap m_obj2contextmap;

    public:
        ExecState(unsigned int kind)
            : m_kind(kind)
            , m_threads()
            , m_time(0)
            , m_uptime(0)
            , m_ccountmap()
            , m_obj2contextmap() {
        }

        // -- Get the current time
        unsigned int TODONow() const { return m_time; }

        // -- Get the current update time
        unsigned int NowUp() const { return m_uptime; }

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

        // Update the Object pointer to simple context pair map
        void UpdateObj2Context( Object *obj, Thread *thread ) {
            assert(obj);
            ContextPair cpair = thread->getContextPair();
            obj->setDeathContextPair( cpair );
            this->m_obj2contextmap[obj] = cpair;
        }

        // -- Map of simple context pair -> count of occurrences
        // TODO: Think about hiding this in an abstraction TODO
        ContextCountMap m_ccountmap;
        ContextCountMap::iterator begin_ccountmap() { return this->m_ccountmap.begin(); }
        ContextCountMap::iterator end_ccountmap() { return this->m_ccountmap.end(); }
};

#endif

