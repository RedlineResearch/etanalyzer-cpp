// ----------------------------------------------------------------------
//  Representation of runtime execution
//  stacks, threads, time

#include "execution.h"

// ----------------------------------------------------------------------
//   Calling context tree

CCNode* CCNode::Call(Method* m)
{
    CCNode* result = 0;
    CCMap::iterator p = m_callees.find(m->getId());
    if (p == m_callees.end()) {
        result = new CCNode( this, // parent
                             m );  // method
        m_callees[m->getId()] = result;
    } else {
        result = (*p).second;
    }
    return result;
}

CCNode* CCNode::Return(Method* m)
{
    if (m_method != m) {
        cout << "WEIRD: Returning from the wrong method " << m->info() << endl;
        cout << "WEIRD:    should be " << m_method->info() << endl;
    }
    return m_parent;
}

string CCNode::info()
{
    stringstream ss;
    if (m_method) {
        ss << m_method->info(); //  << "@" << m_startTime;
    }
    else {
        ss << "ROOT";
    }
    return ss.str();
}

string CCNode::stacktrace()
{
    stringstream ss;
    CCNode* cur = this;
    while (cur) {
        ss << cur->info() << endl;
        cur = cur->getParent();
    }
    return ss.str();
}

// A version that returns a list/vector of
// - method pointers
deque<Method *> CCNode::simple_stacktrace()
{
    if (this->isDone()) {
        return this->m_simptrace;
    }
    deque<Method *> result;
    // Relies on the fact that Method * are unique even
    // if the CCNodes are different as long as they refer
    // to the same method. A different CCNode will mean
    // a different invocation.
    Method *mptr = this->getMethod();
    assert(mptr);
    CCNode *parent_ptr = this->getParent();
    if (parent_ptr) {
        result = parent_ptr->simple_stacktrace();
    }
    result.push_front(mptr);
    this->setDone();
    this->m_simptrace = result;
    return result;
}

bool CCNode::simple_cc_equal( CCNode &other )
{
    // Relies on the fact that Method * are unique even
    // if the CCNodes are different as long as they refer
    // to the same method. A different CCNode will mean
    // a different invocation.
    if (this->getMethod() != other.getMethod()) {
        return false;
    }
    CCNode *self_ptr = this->getParent();
    CCNode *other_ptr = other.getParent();
    if ((self_ptr == NULL || other_ptr == NULL)) {
        return (self_ptr == other_ptr);
    }
    return (self_ptr->simple_cc_equal(*other_ptr));
}


// ----------------------------------------------------------------------
//   Thread representation
//   (Essentially, the stack)

// -- Call method m
void Thread::Call(Method *m)
{
    if (m_kind == 1) {
        CCNode* cur = this->TopCC();
        m_curcc = cur->Call(m);
    }

    if (m_kind == 2) {
        // Save (old_top, new_top) of m_methods
        if (m_methods.size() > 0) {
            m_context = std::make_tuple( m_methods.back(), m );
        } else {
            m_context = std::make_tuple( (Method *) NULL, m );
        }
        ContextCountMap::iterator it = m_ccountmap.find( m_context );
        if (it != m_ccountmap.end()) {
            m_ccountmap[m_context] += 1; 
        } else {
            m_ccountmap[m_context] = 1; 
        }
        m_methods.push_back(m);
        // m_methods, m_locals, and m_deadlocals must be synched in pushing
        // TODO: Do we need to check for m existing in map?
        // Ideally no, but not really sure what is possible in Elephant 
        // Tracks.
        m_locals.push_back(new LocalVarSet());
        m_deadlocals.push_back(new LocalVarSet());
    }
}

// -- Return from method m
void Thread::Return(Method* m)
{
    if (m_kind == 1) {
        CCNode* cur = this->TopCC();
        if (cur->getMethod())
            m_curcc = cur->Return(m);
        else {
            cout << "WARNING: Return from " << m->info() << " at top context" << endl;
            m_curcc = cur;
        }
    }

    if (m_kind == 2) {
        if ( ! m_methods.empty()) {
            Method *cur = m_methods.back();
            m_methods.pop_back();
            // if (cur != m) {
            //     cerr << "WARNING: Return from method " << m->info() << " does not match stack top " << cur->info() << endl;
            // }
            // m_methods, m_locals, and m_deadlocals must be synched in popping

            // NOTE: Maybe refactor. See same code in Thread::Call
            // Save (old_top, new_top) of m_methods
            if (m_methods.size() > 0) {
                // TODO: What if m != cur?
                // It seems reasonable to simply use the m that's passed to us rather than
                // rely on the call stack being correct. TODO: Verify.
                m_context = std::make_tuple( m, m_methods.back() );
            } else {
                m_context = std::make_tuple( m, (Method *) NULL );
            }
            // TODO TODO: Save type (Call vs Return) -- See similar code above.
            ContextCountMap::iterator it = m_ccountmap.find( m_context );
            if (it != m_ccountmap.end()) {
                m_ccountmap[m_context] += 1; 
            } else {
                m_ccountmap[m_context] = 1; 
            }
            // Locals
            LocalVarSet *localvars = m_locals.back();
            m_locals.pop_back();
            LocalVarSet *deadvars = m_deadlocals.back();
            m_deadlocals.pop_back();
            delete localvars;
            delete deadvars;
        } else {
            cout << "ERROR: Stack empty at return " << m->info() << endl;
        }
    }
}

// -- Get current CC
CCNode* Thread::TopCC()
{
    if (m_kind == 1) {
        assert(m_curcc);
        // TODO // -- Create a root context if necessary
        // TODO if (m_curcc == 0) {
        // TODO     m_curcc = new CCNode();
        // TODO }
        return m_curcc;
    }

    if (m_kind == 2) {
        cout << "ERROR: Asking for calling context in stack mode" << endl;
        return 0;
    }

    cout << "ERROR: Unkown mode " << m_kind << endl;
    return 0;
}

// -- Get current method
Method* Thread::TopMethod()
{
    if (m_kind == 1) {
        return TopCC()->getMethod();
    }

    if (m_kind == 2) {
        if ( ! m_methods.empty()) {
            return m_methods.back();
        } else {
            // cout << "ERROR: Asking for top of empty stack" << endl;
            return 0;
        }
    }

    cout << "ERROR: Unkown mode " << m_kind << endl;
    return 0;
}

// -- Get current dead locals
LocalVarSet * Thread::TopLocalVarSet()
{
    if (m_kind == 1) {
        // TODO
        return NULL;
    }
    else if (m_kind == 2) {
        return ((!m_deadlocals.empty()) ? m_deadlocals.back() : NULL);
    }
}

// -- Get a stack trace
string Thread::stacktrace()
{
    if (m_kind == 1) {
        return TopCC()->stacktrace();
    }

    if (m_kind == 2) {
        if ( ! m_methods.empty()) {
            stringstream ss;
            MethodDeque::iterator p;
            for (p = m_methods.begin(); p != m_methods.end(); p++) {
                Method* m =*p;
                ss << m->info() << endl;
            }
            return ss.str();
        } else {
            return "<empty>";
        }
    }

    cout << "ERROR: Unkown mode " << m_kind << endl;
    return "ERROR";
}

// -- Object is a root
void Thread::objectRoot(Object *object)
{
    if (!m_locals.empty()) {
        LocalVarSet *localvars = m_locals.back();
        localvars->insert(object);
    } else {
        cout << "[objectRoot] ERROR: Stack empty at ROOT event." << endl;
    }
}

// -- Check dead object if root
bool Thread::isLocalVariable(Object *object)
{
    if (!m_locals.empty()) {
        LocalVarSet *localvars = m_locals.back();
        LocalVarSet::iterator it = localvars->find(object);
        return (it != localvars->end());
    } else {
        cout << "[isLocalVariable] ERROR: Stack empty at ROOT event." << endl;
        return false;
    }
}

// ----------------------------------------------------------------------
//   Execution state

// -- Look up or create a thread
Thread* ExecState::getThread(unsigned int threadid)
{
    Thread* result = 0;
    ThreadMap::iterator p = m_threads.find(threadid);
    if (p == m_threads.end()) {
        // -- Not there, make a new one
        result = new Thread( threadid,
                             this->m_kind,
                             this->m_ccountmap,
                             *this );
        m_threads[threadid] = result;
    } else {
        result = (*p).second;
    }

    return result;
}

// -- Call method m in thread t
void ExecState::Call(Method* m, unsigned int threadid)
{
    m_time++;
    Thread *t = getThread(threadid);
    if (t) {
        t->Call(m);
    }
}

// -- Return from method m in thread t
void ExecState::Return(Method* m, unsigned int threadid)
{
    m_time++;
    getThread(threadid)->Return(m);
}

// -- Get the top method in thread t
Method* ExecState::TopMethod(unsigned int threadid)
{
    return getThread(threadid)->TopMethod();
}

// -- Get the top calling context in thread t
CCNode* ExecState::TopCC(unsigned int threadid)
{
    return getThread(threadid)->TopCC();
}
