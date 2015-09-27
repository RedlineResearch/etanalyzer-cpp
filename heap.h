#ifndef HEAP_H
#define HEAP_H

// ----------------------------------------------------------------------
//   Representation of objects on the heap
//
#include <iostream>
#include <map>
#include <limits.h>

#include "classinfo.h"

class Object;
class Thread;
class Edge;

typedef map<unsigned int, Object *> ObjectMap;
typedef map<unsigned int, Edge *> EdgeMap;
typedef set<Object *> ObjectSet;
typedef set<Edge *> EdgeSet;

class HeapState
{
public:

  // -- Do ref counting?
  static bool do_refcounting;

  // -- Turn on debugging
  static bool debug;

private:
  // -- Map from IDs to objects
  ObjectMap m_objects;

  // -- Set of edges (all pointers)
  EdgeSet m_edges;

public:
  HeapState()
    : m_objects()
  {}

  Object * allocate(unsigned int id, unsigned int size, char kind, char * type, AllocSite * site, 
                    unsigned int els, Thread * thread, unsigned int create_time);

  Object * get(unsigned int id);

  Edge * make_edge(Object * source, unsigned int field_id, Object * target, unsigned int cur_time);

  ObjectMap::iterator begin() { return m_objects.begin(); }
  ObjectMap::iterator end() { return m_objects.end(); }
  unsigned int size() const { return m_objects.size(); }

  void add_edge(Edge * e) { m_edges.insert(e); }
  EdgeSet::iterator begin_edges() { return m_edges.begin(); }
  EdgeSet::iterator end_edges() { return m_edges.end(); }

  void end_of_program(unsigned int cur_time);
};

class Object
{
private:
  unsigned int m_id;
  unsigned int m_size;
  char m_kind;
  string m_type;
  AllocSite * m_site;
  unsigned int m_elements;
  Thread * m_thread;

  unsigned int m_createTime;
  unsigned int m_deathTime;

  unsigned int m_refCount;

  EdgeMap m_fields;

public:
  Object(unsigned int id, unsigned int size, char kind, char * type, AllocSite * site, 
         unsigned int els, Thread * thread, unsigned int create_time)
    : m_id(id),
      m_size(size),
      m_kind(kind),
      m_type(type),
      m_site(site),
      m_elements(els),
      m_thread(thread),
      m_createTime(create_time),
      m_deathTime(UINT_MAX),
      m_refCount(0)
  {}

  // -- Getters
  unsigned int getId() const { return m_id; }
  unsigned int getSize() const { return m_size; }
  const string& getType() const { return m_type; }
  Thread * getThread() const { return m_thread; }
  unsigned int getCreateTime() const { return m_createTime; }
  unsigned int getDeathTime() const { return m_deathTime; }

  // -- Ref counting
  unsigned int getRefCount() const { return m_refCount; }
  void incrementRefCount() { m_refCount++; }
  void decrementRefCount() { m_refCount--; }

  // -- Access the fields
  const EdgeMap& getFields() const { return m_fields; }

  // -- Get a string representation
  string info();

  // -- Check live
  bool isLive(unsigned int tm) const { return (tm < m_deathTime); }

  // -- Update a field
  void updateField(Edge * edge, unsigned int cur_time);

  // -- Record death time
  void makeDead(unsigned int death_time);
    
};

class Edge
{
private:

  // -- Source object
  Object * m_source;

  // -- Source field
  unsigned int m_sourceField;

  // -- Target object
  Object * m_target;

  // -- Creation time
  unsigned int m_createTime;

  // -- End time
  //    If 0 then ends when source object dies
  unsigned int m_endTime;

public:
  Edge(Object * source, unsigned int field_id, Object * target, unsigned int cur_time)
    : m_source(source),
      m_sourceField(field_id),
      m_target(target),
      m_createTime(cur_time),
      m_endTime(0)
  {}

  Object * getSource() const { return m_source; }
  Object * getTarget() const { return m_target; }
  unsigned int getSourceField() const { return m_sourceField; }
  unsigned int getCreateTime() const { return m_createTime; }
  unsigned int getEndTime() const { return m_endTime; }

  void setEndTime(unsigned int end) { m_endTime = end; }
};


#endif
