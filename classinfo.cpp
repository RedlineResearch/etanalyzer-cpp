#include "classinfo.h"
#include "tokenizer.h"
#include <stdlib.h>

// -- Contents of the names file
ClassMap ClassInfo::TheClasses;

// -- All methods (also in the classes)
MethodMap ClassInfo::TheMethods;
  
// -- All fields (also in the classes)
FieldMap ClassInfo::TheFields;
  
// -- Allocation sites
AllocSiteMap ClassInfo::TheAllocSites;

// -- Debug flag
bool ClassInfo::debug_names = false;

// -- Read in the names file
void ClassInfo::read_names_file(const char * filename)
{
  FILE * f = fopen(filename, "r");
  if ( ! f) {
    cout << "ERROR: Could not open " << filename << endl;
    exit(-1);
  }

  Tokenizer t(f);

  while ( ! t.isDone()) {
    t.getLine();
    if (t.isDone()) break;

    switch (t.getChar(0)) {
    case 'C':
    case 'I':
      {
	// -- Class and interface entries
	// C/I <id> <name> <other stuff>
	// -- Lookup or create the class info...
	Class * cls = 0;
	ClassMap::iterator p = TheClasses.find(t.getInt(1));
	if (p == TheClasses.end()) {
	  // -- Not found, make a new one
	  cls = new Class(t.getInt(1), t.getString(2), (t.getChar(0) == 'I'));
	  TheClasses[cls->getId()] = cls;
	} else {
	  cls = (*p).second;
	}

	if (debug_names)
	  cout << "CLASS " << cls->getName() << " id = " << cls->getId() << endl;

	// Superclass ID and interfaces are optional
	if (t.numTokens() > 3)
	  cls->setSuperclassId(t.getInt(3));

	// -- For now, ignore the rest (interfaces implemented)
      }
      break;

    case 'N': 
      {
	// N <id> <classid> <classname> <methodname> <descriptor> <flags S|I +N>
	// 0  1       2          3           4            5             6
	Class * cls = TheClasses[t.getInt(2)];
	Method * m = new Method(t.getInt(1), cls, t.getString(4), t.getString(5), t.getString(6));
	TheMethods[m->getId()] = m;
	cls->addMethod(m);
	if (debug_names)
	  cout << "   + METHOD " << m->info() << endl;
      }
      break;
      
    case 'F':
      {
	// F S/I <id> <name> <classid> <classname> <descriptor>
	// 0  1   2     3        4          5           6
	Class * cls = TheClasses[t.getInt(4)];
	Field * fld = new Field(t.getInt(2), cls, t.getString(3), t.getString(6), (t.getChar(1) == 'S'));
	TheFields[fld->getId()] = fld;
	cls->addField(fld);
	if (debug_names)
	  cout << "   + FIELD" << fld->getName() << " id = " << fld->getId() << " in class " << cls->getName() << endl;
      }
      break;
      
    case 'S':
      {
	// S <methodid> <classid> <id> <type> <dims>
	// 0    1           2      3     4      5
	Method * meth = TheMethods[t.getInt(1)];
	AllocSite * as = new AllocSite(t.getInt(3), meth, t.getString(3), t.getString(4), t.getInt(5));
	TheAllocSites[as->getId()] = as;
	meth->addAllocSite(as);
	if (debug_names)
	  cout << "   + ALLOC " << as->getType() << " id = " << as->getId() << " in method " << meth->info() << endl;
      }
      break;
      
    case 'E':
      {
	// -- No need to process this entry (end of a class)
      }
      break;
      
    default:
      cout << "ERROR: Unknown char " << t.getChar(0) << endl;
      break;
    }
  }
}

// ----------------------------------------------------------------------
//  Info methods (for debugging)

string Method::info()
{
  stringstream ss;
  ss << m_class->info() << "." << m_name << m_descriptor;
  return ss.str();
}

string AllocSite::info()
{
  stringstream ss;
  ss << m_method->info() << ":" << m_id;
  return ss.str();
}

