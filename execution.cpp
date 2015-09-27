// ----------------------------------------------------------------------
//  Representation of runtime execution
//  stacks, threads, time

#include "execution.h"

// ----------------------------------------------------------------------
//   Calling context tree

CCNode * CCNode::Call(Method * m)
{
  CCNode * result = 0;
  CCMap::iterator p = m_callees.find(m->getId());
  if (p == m_callees.end()) {
    result = new CCNode(this, m);
    m_callees[m->getId()] = result;
  } else {
    result = (*p).second;
  }
  return result;
}

CCNode * CCNode::Return(Method * m)
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
  if (m_method)
    ss << m_method->info(); //  << "@" << m_startTime;
  else
    ss << "ROOT";
  return ss.str();
}

string CCNode::stacktrace()
{
  stringstream ss;
  CCNode * cur = this;
  while (cur) {
    ss << cur->info() << endl;
    cur = cur->getParent();
  }
  return ss.str();
}

// ----------------------------------------------------------------------
//   Thread representation
//   (Essentially, the stack)

// -- Call method m
void Thread::Call(Method * m)
{
  if (m_kind == 1) {
    CCNode * cur = TopCC();
    m_curcc = cur->Call(m);
  }

  if (m_kind == 2) {
    m_methods.push_back(m);
  }
}
  
// -- Return from method m
void Thread::Return(Method * m)
{
  if (m_kind == 1) {
    CCNode * cur = TopCC();
    if (cur->getMethod())
      m_curcc = cur->Return(m);
    else {
      cout << "WARNING: Return from " << m->info() << " at top context" << endl;
      m_curcc = cur;
    }
  }
  
  if (m_kind == 2) {
    if ( ! m_methods.empty()) {
      Method * cur = m_methods.back();
      m_methods.pop_back();
      if (cur != m) {
	cout << "WARNING: Return from method " << m->info() << " does not match stack top " << cur->info() << endl;
      }
    } else {
      cout << "ERROR: Stack empty at return " << m->info() << endl;
    }
  }
}

// -- Get current CC
CCNode * Thread::TopCC()
{
  if (m_kind == 1) {
    // -- Create a root context if necessary
    if (m_curcc == 0) {
      m_curcc = new CCNode();
    }
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
Method * Thread::TopMethod()
{
  if (m_kind == 1) {
    return TopCC()->getMethod();
  }

  if (m_kind == 2) {
    if ( ! m_methods.empty()) {
      return m_methods.back();
    } else {
      cout << "ERROR: Asking for top of empty stack" << endl;
      return 0;
    }
  }

  cout << "ERROR: Unkown mode " << m_kind << endl;
  return 0;
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
	Method * m = *p;
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
  

// ----------------------------------------------------------------------
//   Execution state

// -- Look up or create a thread
Thread * ExecState::getThread(unsigned int threadid)
{
  Thread * result = 0;
  ThreadMap::iterator p = m_threads.find(threadid);
  if (p == m_threads.end()) {
    // -- Not there, make a new one
    result = new Thread(threadid, m_kind);
    m_threads[threadid] = result;
  } else {
    result = (*p).second;
  }

  return result;
}

// -- Call method m in thread t
void ExecState::Call(Method * m, unsigned int threadid)
{
  m_time++;
  getThread(threadid)->Call(m);
}

// -- Return from method m in thread t
void ExecState::Return(Method * m, unsigned int threadid)
{
  m_time++;
  getThread(threadid)->Return(m);
}

// -- Get the top method in thread t
Method * ExecState::TopMethod(unsigned int threadid)
{
  return getThread(threadid)->TopMethod();
}

// -- Get the top calling context in thread t
CCNode * ExecState::TopCC(unsigned int threadid)
{
  return getThread(threadid)->TopCC();
}
