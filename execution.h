#ifndef EXECUTION_H
#define EXECUTION_H

// ----------------------------------------------------------------------
//  Representation of runtime execution
//  stacks, threads, time

#include <iostream>
#include <map>
#include <deque>

#include "classinfo.h"
#include "heap.h"

using namespace std;

// ----------------------------------------------------------------------
//   Calling context tree

class CCNode;
// TODO class CTreeNode;
typedef map<unsigned int, CCNode *> CCMap;
// TODO typedef map<unsigned int, CTreeNode *> CTreeMap;

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

// TODO class CTreeNode
// TODO {
// TODO     private:
// TODO         Method* m_method;
// TODO         CTreeNode* m_parent;
// TODO         CTreeMap m_callees;
// TODO     public:
// TODO         CTreeNode( CTreeNode* parent, Method* m )
// TODO             : m_method(m)
// TODO             , m_parent(parent) {
// TODO         }
// TODO         // -- Get method
// TODO         Method *getMethod() const { return m_method; }
// TODO         // -- Get parent context (if there is one)
// TODO         CTreeNode *getParent() const { return m_parent; }
// TODO         // -- Call a method, making a new child context if necessary
// TODO         CTreeNode *Call(Method *m);
// TODO         // -- Return from a method, returning the parent context
// TODO         CTreeNode *Return(Method *m);
// TODO         // Method name equality
// TODO         bool simple_cc_equal( CTreeNode &other );
// TODO         // TODO
// TODO         deque<Method *> simple_stacktrace();
// TODO };

// ----------------------------------------------------------------------
//   Thread representation

//  Two options for representing the stack
//    (1) Build a full calling context tree
//    (2) Keep a stack of methods
//  (I realize I could probably do this with fancy-dancy OO programming,
//  but sometimes that just seems like overkill

typedef deque<Method *> MethodDeque;
typedef set<Object *> LocalVarSet;
typedef deque<LocalVarSet *> LocalVarDeque;

// TODO typedef unsigned int threadId_t;
// TODO typedef deque< pair<LastEvent, Object*> > LastEventDeque_t;
// TODO typedef map<threadId_t, LastEventDeque_t> LastMap_t;

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
        // -- LastEvent

    public:
        Thread( unsigned int id, unsigned int kind )
            : m_id(id)
            , m_kind(kind)
            , m_rootcc()
            , m_curcc(&m_rootcc) {
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
};

// ----------------------------------------------------------------------
//   Execution state

typedef map<unsigned int, Thread *> ThreadMap;

class ExecState
{
    private:
        // -- Stack kind (CC or methods)
        unsigned int m_kind;
        // -- Set of threads
        ThreadMap m_threads;
        // -- Time
        unsigned int m_time;
    public:
        ExecState(unsigned int kind)
            : m_kind(kind)
            , m_threads()
            , m_time(0) {
        }

        // -- Get the current time
        unsigned int Now() const { return m_time; }

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
};

#endif

