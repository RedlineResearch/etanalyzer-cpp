#include "heap.h"

// -- Global flags
bool HeapState::do_refcounting = true;
bool HeapState::debug = false;

Object * HeapState::allocate(unsigned int id, unsigned int size, char kind, char * type, AllocSite * site, 
                             unsigned int els, Thread * thread, unsigned int create_time)
{
  Object * obj = new Object(id, size, kind, type, site, els, thread, create_time);
  m_objects[obj->getId()] = obj;

  if (m_objects.size() % 100000 == 0)
    cout << "OBJECTS: " << m_objects.size() << endl;

  return obj;
}

// -- Manage heap
Object * HeapState::get(unsigned int id)
{
  ObjectMap::iterator p = m_objects.find(id);
  if (p != m_objects.end())
    return (*p).second;
  else
    return 0;
}

Edge * HeapState::make_edge(Object * source, unsigned int field_id, Object * target, unsigned int cur_time)
{
  Edge * new_edge = new Edge(source, field_id, target, cur_time);
  m_edges.insert(new_edge);

  if (m_edges.size() % 100000 == 0)
    cout << "EDGES: " << m_edges.size() << endl;

  return new_edge;
}

void HeapState::end_of_program(unsigned int cur_time)
{
  // -- Set death time of all remaining live objects
  for (ObjectMap::iterator i = m_objects.begin();
       i != m_objects.end();
       ++i)
    {
      Object * obj = (*i).second;
      if (obj->isLive(cur_time))
        obj->makeDead(cur_time);
    }
}

// -- Return a string with some information
string Object::info() {
  stringstream ss;
  ss << "OBJ 0x" << hex << m_id << dec << "(" << m_type << " " << (m_site != 0 ? m_site->info() : "<NONE>");
  ss << " @" << m_createTime << ")";
  return ss.str();
}

void Object::updateField(Edge * edge, unsigned int cur_time)
{
  unsigned int field_id = edge->getSourceField();
  Object * target = edge->getTarget();

  EdgeMap::iterator p = m_fields.find(field_id);
  if (p != m_fields.end()) {
    // -- Old edge
    Edge * old_edge = (*p).second;
    if (old_edge) {
      // -- Now we know the end time
      old_edge->setEndTime(cur_time);

      // -- Decrement ref count on target
      Object * old_target = old_edge->getTarget();
      if (old_target)
        old_target->decrementRefCount();
    }
  }
  
  // -- Increment new ref
  target->incrementRefCount();
  
  // -- Do store
  m_fields[field_id] = edge;

  if (HeapState::debug)
    cout << "Update " << m_id << "." << field_id << " --> " << target->m_id << " (" << target->getRefCount() << ")" << endl;
}

void Object::makeDead(unsigned int death_time)
{
  // -- Record the death time
  m_deathTime = death_time;
    
  // -- Visit all edges
  for (EdgeMap::iterator p = m_fields.begin();
       p != m_fields.end();
       p++)
    {
      Edge * edge = (*p).second;
      
      // -- Edge dies now
      edge->setEndTime(death_time);
      
      if (HeapState::do_refcounting) {
        // -- Decrement outgoing refs
        Object * target = edge->getTarget();
        if (target)
          target->decrementRefCount();
      }
    }
  
  if (HeapState::debug)
    cout << "Dead object " << m_id << " of type " << m_type << endl;
}

