#ifndef EXECUTION_H
#define EXECUTION_H

// ----------------------------------------------------------------------
//  Representation of runtime execution
//  stacks, threads, time

#include <iostream>
#include <map>
#include <deque>

#include "classinfo.h"

using namespace std;

// ----------------------------------------------------------------------
//   Calling context tree

class CCNode;
typedef map<unsigned int, CCNode *> CCMap;

class CCNode
{
private:
  Method * m_method;
  CCNode * m_parent;
  // unsigned int m_startTime;
  // unsigned int m_endTime;
  // -- Map from method IDs to callee contexts
  CCMap m_callees;

public:
  CCNode()
    : m_method(0),
      m_parent(0)
  {}
      
  CCNode(CCNode * parent, Method * m)
    : m_method(m),
      m_parent(parent)
  {}

  // -- Get method
  Method * getMethod() const { return m_method; }

  // -- Get parent context (if there is one)
  CCNode * getParent() const { return m_parent; }

  // -- Call a method, making a new child context if necessary
  CCNode * Call(Method * m);

  // -- Return from a method, returning the parent context
  CCNode * Return(Method * m);

  // -- Produce a string representation of the context
  string info();

  // -- Generate a stack trace
  string stacktrace();
};

// ----------------------------------------------------------------------
//   Thread representation

//  Two options for representing the stack
//    (1) Build a full calling context tree
//    (2) Keep a stack of methods
//  (I realize I could probably do this with fancy-dancy OO programming,
//  but sometimes that just seems like overkill

typedef deque<Method *> MethodDeque;

class Thread
{
private:
  // -- Thread ID
  unsigned int m_id;

  // -- Kind of stack
  unsigned int m_kind;

  // -- CC tree representation
  CCNode * m_curcc;
  
  // -- Stack of methods
  MethodDeque m_methods;

public:
  Thread(unsigned int id, unsigned int kind)
    : m_id(id),
      m_kind(kind),
      m_curcc(0)
  {}

  unsigned int getId() const { return m_id; }

  // -- Call method m
  void Call(Method * m);
  
  // -- Return from method m
  void Return(Method * m);

  // -- Get current CC
  CCNode * TopCC();

  // -- Get current method
  Method * TopMethod();

  // -- Get a stack trace
  string stacktrace();
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
    : m_kind(kind),
      m_threads(),
      m_time(0)
  {}

  // -- Get the current time
  unsigned int Now() const { return m_time; }

  // -- Look up or create a thread
  Thread * getThread(unsigned int threadid);

  // -- Call method m in thread t
  void Call(Method * m, unsigned int threadid);

  // -- Return from method m in thread t
  void Return(Method * m, unsigned int threadid);

  // -- Get the top method in thread t
  Method * TopMethod(unsigned int threadid);

  // -- Get the top calling context in thread t
  CCNode * TopCC(unsigned int threadid);
};

#endif
  
