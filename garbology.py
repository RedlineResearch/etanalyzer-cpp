# garbology.py
#
import argparse
import os
import sys
import logging
import pprint
import re
import ConfigParser
import csv
import datetime
import subprocess
from collections import defaultdict, Counter
import collections
from functools import reduce
import time
import sqlite3
import cPickle as pickle
import pylru
from sqorm import ObjectCache
from mypytools import hex2dec

pp = pprint.PrettyPrinter( indent = 4 )

#
#  PUBLIC
#
class GarbologyConfig:
    def __init__( self, config_file ):
        self.config_file_name = config_file
        self.config_parser = ConfigParser.ConfigParser()
        self.config_parser.read( config_file )
        self.process_config( config_file )

    def config_section_map( self, section, config_parser ):
        result = {}
        options = config_parser.options(section)
        for option in options:
            try:
                result[option] = config_parser.get(section, option)
            except:
                print("exception on %s!" % option)
                result[option] = None
        return result

    def process_config( self, config_file ):
        cp = self.config_parser
        self.configdict = {}
        cdict = self.configdict
        self.global_cfg = self.config_section_map( "global", cp )
        cdict["global"] = self.global_cfg
        self.etanalyze_cfg = self.config_section_map( "etanalyze-output", cp )
        cdict["etanalyze"] = self.etanalyze_cfg
        self.cycle_analyze_cfg = self.config_section_map( "cycle-analyze", cp )
        cdict["cycle_analyze"] = self.cycle_analyze_cfg
        self.edgeinfo_cfg = self.config_section_map( "edgeinfo", cp )
        cdict["edgeinfo"] = self.edgeinfo_cfg
        self.objectinfo_cfg = self.config_section_map( "objectinfo", cp )
        cdict["objectinfo"] = self.objectinfo_cfg
        self.summary_cfg = self.config_section_map( "summary_cpp", cp )
        cdict["summary"] = self.summary_cfg
        self.dsites_cfg = self.config_section_map( "dsites", cp )
        cdict["dsites"] = self.dsites_cfg

    def verify_all_exist( self, printflag = False ):
        cdict = self.configdict
        basepath = cdict["global"]["cycle_cpp_dir"]
        print "PATH:", basepath
        for key, cfg in self.configdict.iteritems():
            if key == "global" or key == "cycle_analyze":
                continue
            if printflag:
                print "[%s]" % key
            for bmark, relpath in cfg.iteritems():
                tgtpath = basepath + relpath
                if not os.path.isfile(tgtpath):
                    print "ERROR: %s" % str(tgtpath)
                elif printflag:
                    print "%s - OK." % str(tgtpath)

    def print_all_config( self, mypp ):
        print "-------------------------------------------------------------------------------"
        for key, cfg in self.configdict.iteritems():
            print "[%s]" % str(key)
            mypp.pprint( cfg )
            print "-------------------------------------------------------------------------------"

# IMPORTANT: Any changes here, means you have to make the corresponding change
# down in ObjectInfoReader.read_objinfo_file.
def get_index( field = None ):
    try:
        return { "ATIME" : 0,
                 "DTIME" : 1,
                 "SIZE"  : 2,
                 "TYPE"  : 3,
                 "DIEDBY" : 4,
                 "LASTUP" : 5,
                 "STATTR" : 6,
                 "GARBTYPE" : 7,
                 "CONTEXT1" : 8,
                 "CONTEXT2" : 9,
                 "DEATH_CONTEXT_TYPE" : 10,
                 "ALLOC_CONTEXT1" : 11,
                 "ALLOC_CONTEXT2" : 12,
                 "ALLOC_CONTEXT_TYPE" : 13,
                 "ATIME_ALLOC" : 14,
                 "DTIME_ALLOC" : 15,
                 "ALLOCSITE" : 16,
                 "STABILITY" : 17,
                 "LAST_ACTUAL_TS" : 18,
        }[field]
    except:
        return None

def get_raw_index( field = None ):
    return 0 if field == "OBJID" else \
        get_index(field) + 1

def is_key_object( rec = None ):
    return ( rec[get_index("GARBTYPE")] == "CYCKEY" or
             rec[get_index("GARBTYPE")] == "DAGKEY" )

def get_key_objects( idlist = [],
                     object_info_reader = None ):
    oir = object_info_reader
    result = []
    for objId in set(idlist):
        rec = oir.get_record( objId )
        assert( rec != None )
        if is_key_object(rec):
            # print "DBG: [%d] keyobj: %s is %s" % ( objId,
            #                                        oir.get_type_using_typeId(rec[ get_index("TYPE") ]),
            #                                        rec[ get_index("GARBTYPE") ] )
            result.append(rec)
    return list(set(result))

def __cumul_sum__( mysumpair = (0, 0),
                   val = 0 ):
    oldsum = mysumpair[0]
    oldcount = mysumpair[1]
    return (oldsum + val, oldcount + 1)

STABLE = "S"
SERIAL_STABLE = "ST"
def is_stable( attr = "" ):
    return (attr == STABLE)
    # return ( (attr == STABLE) or (attr == SERIAL_STABLE) )

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

class ObjectInfoReader:
    def __init__( self,
                  objinfo_filename = None,
                  db_filename = None,
                  useDB_as_source = False,
                  cachesize = 5000000,
                  deathcontext_mode = "SINGLE",
                  logger = None ):
        self.objinfo_filename = objinfo_filename
        self.objdict = {}
        self.useDB_as_source = useDB_as_source
        self.db_filename = db_filename
        if self.useDB_as_source:
            assert( self.db_filename != None )
        self.typedict = {}
        self.rev_typedict = {}
        self.keyset = set([])
        self.alloc_age_list = []
        self.methup_age_list = []
        self.logger = logger
        # These are computed later on and are simply set to None here
        self.objId2stable_gnum = {}
        self.objId2unstable_gnum = {}
        self.objId2death_gnum = {}
        # The following is the combined stable+death group numbers.
        #    We have it returning None by default for anything that hasn't
        #    been assigned yet.
        self.objId2combined_gnum = defaultdict( lambda: None )
        # Cache size for LRU cache if needed
        self.cachesize = cachesize
        # Death context mode. Current choices are:
        #    SINGLE, PAIR
        assert( deathcontext_mode == "SINGLE" or
                deathcontext_mode == "PAIR" )
        self.__DEATHCONTEXT__ = deathcontext_mode

    def __len__( self ):
        return len(self.objdict)

    def is_key_object( self, objId = None ):
        assert(type(objId) == type(1))
        if objId in self.objdict:
            od = self.objdict
            return ( od[objId][get_index("GARBTYPE")] == "CYCKEY" or
                     od[objId][get_index("GARBTYPE")] == "DAGKEY" )
        else:
            return False

    def raw_objrow_to_list( self, rec = tuple()  ):
        row = [ int(rec[ get_raw_index("ATIME") ]),
                int(rec[ get_raw_index("DTIME") ]),
                int(rec[ get_raw_index("SIZE") ]),
                self.get_typeId( rec[ get_raw_index("TYPE") ] ),
                rec[ get_raw_index("DIEDBY") ],
                rec[ get_raw_index("LASTUP") ],
                rec[ get_raw_index("STATTR") ],
                rec[ get_raw_index("GARBTYPE") ],
                rec[ get_raw_index("CONTEXT1") ], # USED - single death context
                rec[ get_raw_index("CONTEXT2") ], # UNUSED PADDING
                rec[ get_raw_index("DEATH_CONTEXT_TYPE") ], # UNUSED PADDING
                rec[ get_raw_index("ALLOC_CONTEXT1") ], # UNUSED PADDING
                rec[ get_raw_index("ALLOC_CONTEXT2") ], # UNUSED PADDING
                rec[ get_raw_index("ALLOC_CONTEXT_TYPE") ], # UNUSED PADDING
                int(rec[ get_raw_index("ATIME_ALLOC") ]),
                int(rec[ get_raw_index("DTIME_ALLOC") ]),
                rec[ get_raw_index("ALLOCSITE") ],
                rec[ get_raw_index("STABILITY") ],
                int(rec[ get_raw_index("LAST_ACTUAL_TS") ]),
                ]
        return row

    def read_objinfo_file( self,
                           shared_list = None ):
        start = False
        count = 0
        if not self.useDB_as_source:
            done = False
            object_info = self.objdict
            with get_trace_fp( self.objinfo_filename, self.logger ) as fp:
                for line in fp:
                    count += 1
                    line = line.rstrip()
                    if line.find("---------------[ OBJECT INFO") == 0:
                        start = True if not start else False
                        if start:
                            continue
                        else:
                            done = True
                            break
                    if start:
                        rec = line.split(",")
                        objId = int(rec[ get_raw_index("OBJID") ])
                        # IMPORTANT: Any changes here, means you have to make the
                        # corresponding change up in function 'get_index'
                        # The price of admission for a dynamically typed language.
                        row = self.raw_objrow_to_list(rec)
                        self.alloc_age_list.append( ( row[get_index("DTIME_ALLOC")] -
                                                      row[get_index("ATIME_ALLOC")] ) )
                        self.methup_age_list.append( ( row[get_index("DTIME")] -
                                                       row[get_index("ATIME")] ) )
                        if objId not in object_info:
                            object_info[objId] = tuple(row)
                            if self.is_key_object( objId ):
                                self.keyset.add( objId )
                        else:
                            self.logger.error( "DUPE: %s" % str(objId) )
                    # if ( (shared_list != None) and
                    #      (count % 2500 >= 2499) ):
                    #     shared_list.append( count )
            assert(done)
        else:
            self.objdict = ObjectCache( tgtpath = self.db_filename,
                                        table = "objinfo",
                                        keyfield = "objid",
                                        cachesize = self.cachesize,
                                        logger = self.logger )

    def create_objectinfo_db( self, outdbfilename = None ):
        self.outdbfilename = outdbfilename
        # Note that outdbconn will not be closed here.
        try:
            self.outdbconn = sqlite3.connect( outdbfilename )
        except:
            logger.critical( "Unable to open %s" % outdbfilename )
            print "Unable to open %s" % outdbfilename
            exit(1)
        conn = self.outdbconn
        conn.text_factory = str
        cur = conn.cursor()
        # ----------------------------------------------------------------------
        # Create the OBJECTINFO DB
        # ----------------------------------------------------------------------
        cur.execute( '''DROP TABLE IF EXISTS objectinfo''' )
        # Create the database. These are the fields in order.
        # Decode as:
        # num- fullname (sqlite name) : type
        # 1- object Id (objid) : INTEGER
        # 2- allocation time (atime) : INTEGER
        # 3- death time (dtime) : INTEGER
        # 4- size in bytes (size) : INTEGER
        # 5- typeId (typeid) : INTEGER
        # 6- death type (dtype) : TEXT
        #    -- Choices are [S,G,H,E]
        # 7- was last update null (lastnull) : TEXT
        #    -- Choices are [N,V] meaning Null or Value
        # 8- died by stack (diedbystack) : TEXT
        #    -- Choices are [SHEAP,SONLY,H] meaning Stack after Heap, Stack only, Heap
        # 9- dgroup_kind (dgroupkind) : TEXT
        #    -- Choices are [CY,CYKEY,DAG,DAGKEY]
        # 10- death method 1 (dmethod1) : TEXT
        #     -- part 1 of simple context pair - death site
        # 11- death method 2 (dmethod2) : TEXT
        #     -- part 2 of simple context pair - death site
        # 12- death context type (dcontype) : TEXT
        #     -- Choices are [C,R] meaning C is call. R is return.
        # 13- allocation method 1 (amethod1) : TEXT
        #     -- part 1 of simple context pair - alloc site
        # 14- allocation method 2 (amethod2) : TEXT
        #     -- part 2 of simple context pair - alloc site
        # 15- allocation context type (acontype) : TEXT
        #     -- Choices are [C,R]  C is call. R is return.
        # 16- allocation time in bytest allocated (atime_alloc) : INTEGER
        # 17- death time in bytest allocated (dtime_alloc) : INTEGER
        # 18- allocation site (asite_name) : TEXT
        # 19- stability  (stability) : TEXT
        #     -- Choices are [S,U,X] meaning Stable, Unstable, Unknown
        # 20- last actual timestamp (last_actual_ts) : INTEGER
        cur.execute( """CREATE TABLE objinfo (objid INTEGER PRIMARY KEY,
                                              atime INTEGER,
                                              dtime INTEGER,
                                              size INTEGER,
                                              typeid INTEGER,
                                              dtype TEXT,
                                              lastnull TEXT,
                                              diedbystack TEXT,
                                              dgroupkind TEXT,
                                              dmethod1 TEXT,
                                              dmethod2 TEXT,
                                              dcontype TEXT,
                                              amethod1 TEXT,
                                              amethod2 TEXT,
                                              acontype TEXT,
                                              atime_alloc INTEGER,
                                              dtime_alloc INTEGER,
                                              asite_name TEXT,
                                              stability TEXT,
                                              last_actual_ts INTEGER)""" )
        conn.execute( 'DROP INDEX IF EXISTS idx_objectinfo_objid' )
        # Now create the type table which maps:
        #     typeId -> actual type
        cur.execute( '''DROP TABLE IF EXISTS typetable''' )
        # Create the database. These are the fields in order.
        # Decode as:
        # num- fullname (sqlite name) : type
        # 1- type Id (typeid) : INTEGER
        # 2- type  (type) : TEXT
        cur.execute( """CREATE TABLE typetable (typeid INTEGER PRIMARY KEY,
                                                type TEXT)""" )
        conn.execute( 'DROP INDEX IF EXISTS idx_typeinfo_typeid' )

    def read_objinfo_file_into_db( self ):
        # Declare our generator
        # ----------------------------------------------------------------------
        def row_generator():
            start = False
            count = 0
            with get_trace_fp( self.objinfo_filename, self.logger ) as fp:
                for line in fp:
                    count += 1
                    line = line.rstrip()
                    if line.find("---------------[ OBJECT INFO") == 0:
                        start = True if not start else False
                        if start:
                            continue
                        else:
                            break
                    if start:
                        rec = line.split(",")
                        objId = int(rec[ get_raw_index("OBJID") ])
                        # IMPORTANT: Any changes here, means you have to make the
                        # corresponding change up in function 'get_index'
                        # The price of admission for a dynamically typed language.
                        row = self.raw_objrow_to_list(rec)
                        row.insert( 0, objId )
                        # DEBUG: print ">> %s" % str(row)
                        yield tuple(row)

        def type_row_generator():
            for mytype, typeId in self.typedict.items():
                # DEBUG: print "%d -> %s" % (typeId, mytype)
                yield (typeId, mytype)
        # ----------------------------------------------------------------------
        # TODO call executemany here
        cur = self.outdbconn.cursor()
        cur.executemany( "INSERT INTO objinfo VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row_generator() )
        cur.executemany( "INSERT INTO typetable VALUES (?,?)", type_row_generator() )
        cur.execute( 'CREATE UNIQUE INDEX idx_objectinfo_objid ON objinfo (objid)' )
        cur.execute( 'CREATE UNIQUE INDEX idx_typeinfo_typeid ON typetable (typeid)' )
        self.outdbconn.commit()
        self.outdbconn.close()

    def get_typeId( self, mytype ):
        typedict = self.typedict
        rev_typedict = self.rev_typedict
        if mytype in typedict:
            return typedict[mytype]
        else:
            if not self.useDB_as_source:
                lastkey = len(typedict.keys())
                typedict[mytype] = lastkey + 1
                rev_typedict[lastkey + 1] = mytype
                return lastkey + 1
            else:
                rec = self.objdict.getitem_from_table( mytype, "typetable", "type" )
                rev_typedict[rec[0]] = mytype
                return rec[0] if (rec != None) else None

    def get_type( self, objId = 0 ):
        rec = self.get_record(objId)
        typeId = rec[ get_index("TYPE") ] if rec != None else None
        if not self.useDB_as_source:
            return self.rev_typedict[typeId] if typeId != None \
                else "NONE"
        else:
            if typeId not in self.rev_typedict:
                rec = self.objdict.getitem_from_table( typeId, "typetable", "typeid" )
                if rec != None:
                    mytype = rec[1]
                    self.rev_typedict[typeId] = mytype
                    return mytype
                else:
                    return "NONE"
            else:
                return self.rev_typedict[typeId]

    def died_at_end( self, objId ):
        return (self.objdict[objId][get_index("DIEDBY")] == "E") if (objId in self.objdict) \
            else False

    def get_death_cause( self, objId ):
        rec = self.get_record(objId)
        return self.get_death_cause_using_record(rec)

    def get_death_cause_using_record( self, rec = None ):
        return rec[get_index("DIEDBY")] if (rec != None) \
            else "NONE"

    def get_last_actual_timestamp( self, objId ):
        rec = self.get_record(objId)
        return self.get_last_actual_timestamp_using_record(rec)

    def get_last_actual_timestamp_using_record( self, rec = None ):
        return rec[get_index("LAST_ACTUAL_TS")] if (rec != None) \
            else 0

    def iteritems( self ):
        """Returns (objId, rec) where rec is the record for the object with that object id."""
        return self.objdict.iteritems()

    def iterrecs( self ):
        """Returns (objId, rec) where rec is the record for the object with that object id."""
        return self.objdict.iteritems()

    def keys( self ):
        return self.objdict.keys()

    # If numlines == 0, print out all.
    def print_out( self, numlines = 30 ):
        count = 0
        for objId, rec in self.objdict.iteritems():
            print "%d -> %s" % (objId, str(rec))
            count += 1
            if numlines != 0 and count >= numlines:
                break

    def get_record( self, objId = 0 ):
        return self.objdict[objId] if (objId in self.objdict) else None

    def get_size( self, objId = 0 ):
        rec = self.get_record(objId)
        return rec[ get_index("SIZE") ] if rec != None else None

    def get_type_using_typeId( self, typeId = 0 ):
        return self.rev_typedict[typeId] if typeId in self.rev_typedict \
            else "NONE"

    def get_death_time( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_death_time_using_record(rec)

    def get_death_time_ALLOC( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_death_time_using_record_ALLOC(rec)

    def get_alloc_time_using_record( self, rec = None ):
        return rec[ get_index("ATIME") ] if rec != None else 0

    def get_alloc_time( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_alloc_time_using_record(rec)

    # This weirdly named function gets the allocation time of an object
    # using the logical allocation time (in bytes allocated) as the
    # basis for time. I know it sounds weird
    def get_alloc_time_using_record_ALLOC( self, rec = None ):
        return rec[ get_index("ATIME_ALLOC") ] if rec != None else 0

    def get_alloc_time_ALLOC( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_alloc_time_using_record_ALLOC(rec)

    def get_age_using_record( self, rec = None ):
        return ( self.get_death_time_using_record(rec) - \
                 self.get_alloc_time_using_record(rec) ) \
            if rec != None else 0

    def get_age( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_age_using_record(rec)

    # The *_ALLOC versions means that we're using allocation time
    # instead of our standard logical (method + update) time.
    def get_age_using_record_ALLOC( self, rec = None ):
        return ( self.get_death_time_using_record_ALLOC(rec) - \
                 self.get_alloc_time_using_record_ALLOC(rec) ) \
            if rec != None else 0

    def get_age_ALLOC( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_age_using_record_ALLOC(rec)

    def get_death_time_using_record( self, rec = None ):
        return rec[ get_index("DTIME") ] if rec != None else 0

    # This weirdly named function gets the death time of an object
    # using the logical allocation time (in bytes allocated) as the
    # basis for time.
    def get_death_time_using_record_ALLOC( self, rec = None ):
        return rec[ get_index("DTIME_ALLOC") ] if rec != None else 0

    def is_array( self, objId = 0 ):
        typeId = self.get_record(objId)[ get_index("TYPE") ]
        return self.rev_typedict[typeId][0] == "["

    def died_by_stack( self, objId = 0 ):
        return (self.objdict[objId][get_index("DIEDBY")] == "S") if (objId in self.objdict) \
            else False

    def died_by_heap( self, objId = 0 ):
        return (self.objdict[objId][get_index("DIEDBY")] == "H") if (objId in self.objdict) \
            else False

    def died_by_global( self, objId = 0 ):
        return (self.objdict[objId][get_index("DIEDBY")] == "G") if (objId in self.objdict) \
            else False

    def died_by_program_end( self, objId = 0 ):
        return (self.objdict[objId][get_index("DIEDBY")] == "E") if (objId in self.objdict) \
            else False

    def group_died_by_stack( self, grouplist = [] ):
        for obj in grouplist:
            # Shortcircuits the evaluation
            if not self.died_by_stack(obj):
                return False
        return True

    def verify_died_by( self,
                        grouplist = [],
                        died_by = None,
                        fail_on_missing = False ):
        assert( died_by == "S" or died_by == "H" or died_by == "E" or died_by == "G" )
        flag = True
        for obj in grouplist:
            if obj not in self.objdict:
                self.logger.critical( "Missing object: %d" % obj )
                if fail_on_missing:
                    return False
                continue
            else:
                rec = self.objdict[obj]
                if rec[ get_index("DIEDBY") ] != died_by:
                    self.logger.error( "Looking for '%s' - found '%s'" %
                                       (died_by, rec[ get_index("DIEDBY") ]) )
                    flag = False
        return flag

    # Death context functions
    def get_death_context( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_death_context_record(rec)

    def get_death_context_record( self, rec = None ):
        if self.__DEATHCONTEXT__ == "SINGLE":
            pass
        elif self.__DEATHCONTEXT__ == "PAIR":
            first = rec[ get_index("CONTEXT1") ] if rec != None else "NONE"
            second = rec[ get_index("CONTEXT2") ] if rec != None else "NONE"
            return (first, second)
        else:
            raise ValueError("Unknown DEATHCONTEXT mode: %s" % str(self.__DEATHCONTEXT__))

    def get_death_context_type( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_death_context_type_using_record(rec)

    def get_death_context_type_using_record( self, rec = None ):
        return rec[ get_index("DEATH_CONTEXT_TYPE") ] if rec != None else "NONE"

    # Alloc context functions
    def get_alloc_context( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_alloc_context_record(rec)

    def get_alloc_context_record( self, rec = None ):
        first = rec[ get_index("ALLOC_CONTEXT1") ] if rec != None else "NONE"
        second = rec[ get_index("ALLOC_CONTEXT2") ] if rec != None else "NONE"
        return (first, second)

    def get_alloc_context_type( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_alloc_context_type_using_record(rec)

    def get_alloc_context_type_using_record( self, rec = None ):
        return rec[ get_index("ALLOC_CONTEXT_TYPE") ] if rec != None else "NONE"

    # Allocsite functions
    def get_allocsite( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_allocsite_using_record(rec)

    def get_allocsite_using_record( self, rec = None ):
        return rec[ get_index("ALLOCSITE") ] if rec != None else "NONE"
        # TODO TODO
        # return rec[ get_index("ALLOC_CONTEXT1") ] if rec != None else "NONE"

    def get_stack_died_by_attr( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_stack_died_by_attr_using_record(rec)

    def get_stack_died_by_attr_using_record( self, rec = None ):
        return rec[ get_index("STATTR") ] if rec != None else "NONE"

    def get_last_heap_update( self, objId = 0 ):
        rec = self.get_record(objId)
        return self.get_last_heap_update_using_record(rec)

    def get_last_heap_update_using_record( self, rec = None ):
        return rec[ get_index("LASTUP") ] if rec != None else "NONE"

    def get_alloc_age_list( self ):
        return self.alloc_age_list

    def get_methup_age_list( self ):
        return self.methup_age_list

    def set_stable_group_number( self,
                                 objId = None,
                                 gnum = None ):
        self.objId2stable_gnum[objId] = gnum

    def set_unstable_group_number( self,
                                   objId = None,
                                   gnum = None ):
        self.objId2unstable_gnum[objId] = gnum

    def set_combined_sd_group_number( self,
                                      objId = None,
                                      gnum = None ):
        self.objId2combined_gnum[objId] = gnum

    def get_stable_group_number( self,
                                 objId = None ):
        return self.objId2stable_gnum[objId] if objId in self.objId2stable_gnum else None

    def get_unstable_group_number( self,
                                   objId = None ):
        return self.objId2unstable_gnum[objId] if objId in self.objId2unstable_gnum else None

    def get_comibined_sd_group_number( self,
                                       objId = None ):
        return self.objId2combined_gnum[objId]
        # Note this is a default dict that returns None if it isn't in the dictionary.
        # I don't know if this is better or if checking is better (see get_stable_group_number).

    def set_death_group_number( self,
                                objId = None,
                                gnum = None ):
        self.objId2death_gnum[objId] = gnum

    def get_death_group_number( self,
                                objId = None ):
        return self.objId2death_gnum[objId] if objId in self.objId2death_gnum else None

    def __contains__( self, item ):
        """Return if ObjectReader contains item (which is an object Id)"""
        return item in self.objdict

class ObjectInfoFile2DB:
    def __init__( self,
                  objinfo_filename = "",
                  outdbfilename = "",
                  logger = None ):
        assert( logger != None )
        assert( os.path.isfile(objinfo_filename) )
        self.objreader = ObjectInfoReader( objinfo_filename = objinfo_filename,
                                           useDB_as_source = False,
                                           logger = logger )
        self.objreader.create_objectinfo_db( outdbfilename = outdbfilename )
        self.objreader.read_objinfo_file_into_db()


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# EdgeInfoReader related classes

EdgeInfoTable = "edgeinfo"
EdgeInfoKeyField = "srcid"
LastEdgeTable = "lastedge"
LastEdgeKeyField = "tgtid"

def get_edgeinfo_db_filename( workdir = None,
                              bmark = None ):
    assert( os.path.isdir(workdir) )
    assert( type(bmark) is str )
    return os.path.join( workdir, bmark + "-EDGEINFO.db" )

def get_src_rows( src = None,
                  cursor = None ):
    """Get all rows from edgeinfo table with source = src.
       Caller must send a cursor into the SQLite DB.
       Returns a list."""
    global EdgeInfoTable
    etable = EdgeInfoTable
    assert(type(src) is int)
    result = []
    cursor.execute( "SELECT * FROM %s WHERE srcid=%d" %
                    (etable, src) )
    for row in cursor:
        mylist.append( row )
    return mylist

def get_tgt_rows( tgt = None,
                  cursor = None ):
    """Get all rows from edgeinfo table with source = tgt.
       Caller must send a cursor into the SQLite DB.
       Returns a list."""
    global EdgeInfoTable
    assert(type(tgt) is int)
    result = []
    cursor.execute( "SELECT * FROM %s WHERE tgtid=%d" %
                    (EdgeInfoTable, tgt) )
    while True:
        reclist = cursor.fetchmany()
        if len(reclist) > 0:
            for row in reclist:
                result.append( row )
        else:
            break
    return result 

def get_lastedge_rec_from_DB( tgt = None,
                              cursor = None ):
    """Get all rows from lastedge table with source = tgt.
       Caller must send a cursor into the SQLite DB.
       Returns a list."""
    global LastEdgeTable
    ltable = LastEdgeTable
    assert(type(tgt) is int)
    result = []
    cursor.execute( "SELECT * FROM %s WHERE tgtid=%d" %
                    (ltable, tgt) )
    for row in cursor:
        mylist.append( row )
    return mylist

class EdgeInfoReader:
    # C++ enums from heap.h
    # enum class EdgeState
    #     : std::uint8_t {
    #         NONE = 1,
    #         LIVE = 2,
    #         DEAD_BY_UPDATE = 3,
    #         DEAD_BY_OBJECT_DEATH = 4,
    #         DEAD_BY_PROGRAM_END = 5
    # };
    ES_NONE = 1
    ES_LIVE = 2
    ES_DEAD_BY_UPDATE = 3
    DEAD_BY_OBJECT_DEATH = 4
    DEAD_BY_PROGRAM_END = 5

    ES2STR = { ES_NONE : "NONE",
               ES_LIVE : "LIVE",
               ES_DEAD_BY_UPDATE : "DEAD_BY_UPDATE",
               DEAD_BY_OBJECT_DEATH : "BY_OBJECT_DEATH",
               DEAD_BY_PROGRAM_END : "BY_PROGRAM_END",
    }

    def __init__( self,
                  edgeinfo_filename = None,
                  edgedb_filename = None,
                  stabreader = None,
                  useDB_as_source = False,
                  cachesize = 5000000,
                  logger = None ):
        # TODO: Choice of loading from text file or from pickle
        #
        self.edgeinfo_file_name = edgeinfo_filename
        # Use an SQLITE as source instead of the EDGEINFO file
        self.useDB_as_source = useDB_as_source
        # Edge dictionary
        if not self.useDB_as_source:
            self.edgedict = {} # (src, tgt) -> (create time, death time)
        else:
            self.edge_srclru = pylru.lrucache( size = cachesize )
            self.edge_tgtlru = pylru.lrucache( size = cachesize )
            self.dbconn = sqlite3.connect( edgedb_filename )
            self.lastedge_pickle = edgedb_filename + ".pickle"
        # Source to target object dictionary
        self.srcdict = defaultdict( set ) # src -> set of tgts
        # Target to incoming source object dictionary
        self.tgtdict = defaultdict( set ) # tgt -> set of srcs
        # Target object to record of last edge
        self.lastedge = {} # tgt -> (list of lastedges, death time)
        # Stability reader
        self.stabreader = stabreader
        self.logger = logger

    def read_edgeinfo_file( self ):
        assert( not self.useDB_as_source )
        start = False
        done = False
        with get_trace_fp( self.edgeinfo_file_name, self.logger ) as fp:
            for line in fp:
                line = line.rstrip()
                if line.find("---------------[ EDGE INFO") == 0:
                    start = True if not start else False
                    if start:
                        continue
                    else:
                        done = True
                        break
                if start:
                    rowtmp = line.split(",")
                    # 0 - srcId
                    # 1 - tgtId
                    # 2 - create time
                    # 3 - death time
                    # 4 - fieldId
                    # 5 - edgestate
                    row = [ int(x) for x in rowtmp ]
                    src = row[0]
                    tgt = row[1]
                    timepair = tuple(row[2:4])
                    dtime = row[3]
                    fieldId = row[4]
                    edgestate = row[5]
                    # TODO MAKE INTO A LIST? TODO
                    # TODO self.edgedict[tuple([src, fieldId, tgt])] = { "tp" : timepair,
                    # TODO                                               "s" : "X" }  # X means unknown
                    key = tuple([src, fieldId, tgt])
                    value = { "tp" : timepair,
                              "s" : "X",
                              "es" : edgestate } # X means unknown
                    if key not in self.edgedict:
                        self.edgedict[key] = [ value, ]
                    else:
                        self.edgedict[key].append(value)
                    # END TODO. see above
                    self.srcdict[src].add( tgt )
                    self.tgtdict[tgt].add( src )
                    self.update_last_edges( src = src,
                                            tgt = tgt,
                                            deathtime = dtime )
        assert(done)

    def read_edgeinfo_file_into_db( self ):
        raise RuntimeError("TODO: Need to implement.")

    def update_stability_reader( self,
                                 stabreader = None ):
        assert( stabreader != None )
        self.stabreader = stabreader

    # Read the edgeinfo file to SAVE INTO the SQlite DB.
    def read_edgeinfo_file_with_stability_into_db( self,
                                                   stabreader = None ):
        try:
            assert( stabreader != None )
        except:
            raise ValueError( "Stability reader should be set." )
        # Declare our generator
        # ----------------------------------------------------------------------
        def row_generator():
            sb = self.stabreader
            start = False
            count = 0
            with get_trace_fp( self.edgeinfo_file_name, self.logger ) as fp:
                for line in fp:
                    line = line.rstrip()
                    if line.find("---------------[ EDGE INFO") == 0:
                        start = True if not start else False
                        if start:
                            continue
                        else:
                            break
                    if start:
                        rowtmp = line.split(",")
                        # 0 - srcId
                        # 1 - tgtId
                        # 2 - create time
                        # 3 - death time
                        # 4 - fieldId
                        # 5 - edgestate
                        row = [ int(x) for x in rowtmp ]
                        src = row[0]
                        tgt = row[1]
                        ctime = row[2]
                        dtime = row[3]
                        fieldId = row[4]
                        edgestate = row[5]
                        newrow = [ src, fieldId, tgt, ctime, dtime, edgestate ]
                        # timepair = tuple(row[2:4])
                        try:
                            stability = sb[src][fieldId]
                        except:
                            stability = "X" # X means unknown
                        newrow.append(stability)
                        newrow.append(edgestate)
                        self.update_last_edges( src = src,
                                                tgt = tgt,
                                                deathtime = dtime )
                        yield tuple(newrow)
            # End generator
        #================================================================================
        cur = self.outdbconn.cursor()
        cur.executemany( "INSERT INTO edgeinfo VALUES (?,?,?,?,?,?,?)", row_generator() )
        cur.execute( 'CREATE INDEX idx_edgeinfo_srcid ON edgeinfo (srcid)' )
        cur.execute( 'CREATE INDEX idx_edgeinfo_tgtid ON edgeinfo (tgtid)' )
        #================================================================================
        # Now save the lastedge dictionary as an SQlite DB
        def row_generator_lastedge():
            for tgt, mydict in self.lastedge.iteritems():
                if len(mydict["lastsources"]) > 0:
                    yield (tgt, mydict["dtime"], str(mydict["lastsources"]))
        cur.executemany( "INSERT INTO lastedge VALUES (?,?,?)", row_generator_lastedge() )
        cur.execute( 'CREATE INDEX idx_lastedge_tgtid ON lastedge (tgtid)' )
        # Save lastedge dictionary as a pickle too
        self.save_lastedges_to_pickle( self.outdbfilename + ".pickle" )
        #================================================================================
        # Close the connection
        self.outdbconn.commit()
        self.outdbconn.close()

    # Read the edgeinfo file for USE with the stability information.
    def read_edgeinfo_file_with_stability( self,
                                           stabreader = {} ):
        if not self.useDB_as_source:
            start = False
            done = False
            sb = stabreader
            with get_trace_fp( self.edgeinfo_file_name, self.logger ) as fp:
                for line in fp:
                    line = line.rstrip()
                    if line.find("---------------[ EDGE INFO") == 0:
                        start = True if not start else False
                        if start:
                            continue
                        else:
                            done = True
                            break
                    if start:
                        rowtmp = line.split(",")
                        # 0 - srcId
                        # 1 - tgtId
                        # 2 - create time
                        # 3 - death time
                        # 4 - fieldId
                        row = [ int(x) for x in rowtmp ]
                        src = row[0]
                        tgt = row[1]
                        timepair = tuple(row[2:4])
                        dtime = row[3]
                        fieldId = row[4]
                        try:
                            stability = sb[src][fieldId]
                        except:
                            stability = "X" # X means unknown
                        key = tuple([src, fieldId, tgt])
                        if key not in self.edgedict:
                            self.edgedict[key] = [ { "tp" : timepair,
                                                     "s" : stability }, ]
                        else:
                            self.edgedict[key].append( { "tp" : timepair,
                                                         "s" : stability } )
                        self.srcdict[src].add( tgt )
                        self.tgtdict[tgt].add( src )
                        self.update_last_edges( src = src,
                                                tgt = tgt,
                                                deathtime = dtime )
            assert(done)
        # else:
        #     The edge_srclru and edge_tgtlru have all been configured in __init__()
        #     TODO: DEBUG only raise RuntimeError("TODO: Not yet implemented.")

    def save_lastedges_to_pickle( self, lastedge_pickle = None ):
        with open( lastedge_pickle, "wb" ) as fptr:
            pickle.dump( self.lastedge, fptr )

    def get_targets( self, src = 0 ):
        if not self.useDB_as_source:
            return self.srcdict[src] if (src in self.srcdict) else []
        else:
            if src in self.srcdict:
                return self.srcdict[src]
            else:
                # Get all records from SQlite DB
                reclist = get_src_rows( src, self.dbconn.cursor() )
                #  - save all the records in LRU
                if src not in self.edge_srclru:
                    self.edge_srclru[src] = reclist
                #  - get all targets from the rows
                tgtlist = [ x[2] for x in reclist ]
                self.srcdict[src] = tgtlist
                return list(tgtlist)

    def get_edge_targets( self, src = 0 ):
        if not self.useDB_as_source:
            raise RuntimeError("TODO: To be implemented")
            # return self.srcdict[src] if (src in self.srcdict) else []
        else:
            raise RuntimeError("TODO: To be implemented")
            # if src in self.srcdict:
            #     return self.srcdict[src]
            # else:
            #     # Get all records from SQlite DB
            #     reclist = get_src_rows( src, self.dbconn.cursor() )
            #     #  - save all the records in LRU
            #     if src not in self.edge_srclru:
            #         self.edge_srclru[src] = reclist
            #     #  - get all targets from the rows
            #     tgtlist = [ x[2] for x in reclist ]
            #     self.srcdict[src] = tgtlist
            #     return list(tgtlist)

    def get_edge_sources( self, tgt = 0 ):
        if not self.useDB_as_source:
            raise RuntimeError("TODO: To be implemented")
            return self.tgtdict[tgt] if (tgt in self.tgtdict) else []
        else:
            raise RuntimeError("TODO: To be implemented")
            if tgt in self.tgtdict:
                return self.tgtlru[tgt]
            else:
                # Get all records from SQlite DB
                reclist = get_tgt_rows( tgt, self.dbconn.cursor() )
                #  - save all the records in LRU
                if tgt not in self.edge_tgtlru:
                    self.edge_tgtlru[tgt] = reclist
                #  - get all targets from the rows
                srclist = [ self.get_source_id_from_rec(x) for x in reclist ]
                self.tgtdict[tgt] = srclist
                # NOTE: We return reclist, NOT srclist.
                #       We want the whole record here, not just the source object.
                return list(reclist)

    def get_sources_records( self, tgt = 0 ):
        if not self.useDB_as_source:
            raise ValueError("This shouldn't be called if not using an SQlite DB.")
        else:
            if tgt in self.edge_tgtlru:
                return  self.edge_tgtlru[tgt]
            # Get all records from SQlite DB
            reclist = get_tgt_rows( tgt, self.dbconn.cursor() )
            #  - save all the records in LRU
            if tgt not in self.edge_tgtlru:
                self.edge_tgtlru[tgt] = reclist
            #  - get all targets from the rows
            srclist = [ x[2] for x in reclist ]
            self.tgtdict[tgt] = srclist
            return list(reclist)

    def get_sources( self, tgt = 0 ):
        if not self.useDB_as_source:
            return self.tgtdict[tgt] if (tgt in self.tgtdict) else []
        else:
            if tgt not in self.tgtdict:
                self.get_sources_records(tgt)
                # Side effect of this call is to populate the appropriate entry
                # in the tgtdict dictionary.
            return self.tgtdict[tgt]

    def edgedict_iteritems( self ):
        if not self.useDB_as_source:
            return self.edgedict.iteritems()
        else:
            raise RuntimeError("edgedict_iteritems() isn't defined when using an SQlite DB.")

    #----------------------------------------------------------------------
    # src dictionary related functions
    def srcdict_iteritems( self ):
        if not self.useDB_as_source:
            return self.srcdict.iteritems()
        else:
            # For DB situations, I take this to mean the srclru.
            raise RuntimeError("srcdict_iteritems() isn't defined when using an SQlite DB. Try using srcdict_items()")

    def srcdict_items( self ):
        if not self.useDB_as_source:
            return self.srcdict.items()
        else:
            # For DB situations, I take this to mean the srclru.
            return self.srclru.items()

    #----------------------------------------------------------------------
    # tgt dictionary related functions
    def tgtdict_iteritems( self ):
        if not self.useDB_as_source:
            return self.tgtdict.iteritems()
        else:
            # For DB situations, I take this to mean the tgtlru.
            raise RuntimeError("tgtdict_iteritems() isn't defined when using an SQlite DB. Try using tgtdict_items()")

    def tgtdict_items( self ):
        if not self.useDB_as_source:
            return self.tgtdict.iteritems()
        else:
            # For DB situations, I take this to mean the tgtlru.
            return self.tgtlru.items()

    def lastedge_iteritems( self ):
        return self.lastedge.iteritems()

    def print_out( self, numlines = 30 ):
        count = 0
        for edge, rec in self.edgedict.iteritems():
            timepair = rec["tp"]
            print "(%d[ %d ], %d) -> %s" % (edge[0], edge[1], edge[2], str(timepair))
            count += 1
            if numlines != 0 and count >= numlines:
                break

    def get_edge_timepair( self, edge = None ):
        """The parameter 'edge' is a tuple (src object, field Id, tgt object).
        Returns the (alloc, death) pair.
        """
        if edge in self.edgedict:
            return self.edgedict[ edge ]["tp"]
        else:
            return None

    def update_last_edges( self,
                           src = None,
                           tgt = None,
                           deathtime = None ):
        # Given a target, find what the sources are
        if tgt in self.lastedge:
            if self.lastedge[tgt]["dtime"] < deathtime:
                self.lastedge[tgt] = { "lastsources" : [ src ],
                                       "dtime" : deathtime }
            elif self.lastedge[tgt]["dtime"] == deathtime:
                self.lastedge[tgt]["lastsources"].append(src)
        else:
            self.lastedge[tgt] = { "lastsources" : [ src ],
                                   "dtime" : deathtime }

    def in_lastedge_dict( self, tgtId = None ):
        if not self.useDB_as_source:
            return tgtId in self.lastedge
        else:
            assert(False) # TODO TODO TODO

    def get_last_edge_record( self, tgtId = None ):
        if not self.useDB_as_source:
            return self.lastedge[tgtId] if tgtId in self.lastedge else None
        else:
            reclist = get_lastedge_rec_from_DB( tgt = tgtId,
                                                cursor = self.dbconn.cursor() )
            assert( len(reclist) <= 1 )
            # The record looks like this:
            # 1- target Id (tgtid) : INTEGER
            # 2- death time (dtime) : INTEGER
            # 3- last source list (srclist) : TEXT
            #     NOTE: see create_edgeinfo_db and keep this doc updated
            if len(reclist) == 1:
                rec = reclist[0]
                lastsources = [ int(x) for x in rec[2].split(",") ]
                return { "lastsources" : lastsources,
                         "dtime" : rec[1] }
            else:
                # No last edge
                return None

    def create_edgeinfo_db( self, outdbfilename = None ):
        global EdgeInfoTable
        self.outdbfilename = outdbfilename
        try:
            self.outdbconn = sqlite3.connect( outdbfilename )
        except:
            logger.critical( "Unable to open %s" % outdbfilename )
            print "Unable to open %s" % outdbfilename
            exit(1)
        conn = self.outdbconn
        conn.text_factory = str
        cur = conn.cursor()
        # ----------------------------------------------------------------------
        # Create the EDGEINFO DB
        # ----------------------------------------------------------------------
        cur.execute( '''DROP TABLE IF EXISTS edgeinfo''' )
        # Create the database. These are the fields in order.
        # Decode as:
        # num- fullname (sqlite name) : type
        # 1- source Id (srcid) : INTEGER
        # 2- source field (srcfield) : INTEGER
        # 3- target Id (tgtId) : INTEGER
        # 4- create time (ctime) : INTEGER
        # 5- death time (dtime) : INTEGER
        # 6- stability (stability) : TEXT
        # 7- edge state (edgestate) : TEXT
        cur.execute( """CREATE TABLE %s (srcid INTEGER,
                                         srcfield INTEGER,
                                         tgtid INTEGER,
                                         ctime INTEGER,
                                         dtime INTEGER,
                                         stability TEXT,
                                         edgestate TEXT,
                                         UNIQUE (srcid, tgtid, srcfield, ctime))"""  % EdgeInfoTable )
        conn.execute( 'DROP INDEX IF EXISTS idx_edgeinfo_srcid' )
        conn.execute( 'DROP INDEX IF EXISTS idx_edgeinfo_tgtid' )
        #
        # Now the lastedge table
        cur.execute( '''DROP TABLE IF EXISTS lastedge''' )
        # Create the database. These are the fields in order.
        # Decode as:
        # num- fullname (sqlite name) : type
        # 1- target Id (tgtid) : INTEGER
        # 2- death time (dtime) : INTEGER
        # 3- last source list (srclist) : TEXT
        #    * NOTE: This is a comma separated list of object Ids
        #            It is formatted like the Python list
        #              Example:  [1,2,3]
        cur.execute( """CREATE TABLE lastedge (tgtid INTEGER PRIMARY KEY,
                                               dtime INTEGER,
                                               srclist TEXT)""" )
        conn.execute( 'DROP INDEX IF EXISTS idx_lastedge_tgtid' )

    def __save_in_srclru__( self,
                            src = None,
                            reclist = [] ):
        assert(False) # TODO TODO: DELETE?
        # srclru = 
        # tgtlru = self.edge_tgtlru
        # self.dbconn = sqlite3.connect( edgedb_filename )

    # Decipher the row record from EdgeInfoReader SQlite DB.
    # I decided to make this a part of the EdgeInfoReader.
    # To call the functions, use the static form like so:
    #      EdgeInfoFileReader.get_source_id_from_rec
    # Layout is as follows. You need to keep this in sync with the
    # DB format if it changes below in EdgeInfoReader.
    #     1- source Id (srcid) : INTEGER
    #     2- source field (srcfield) : INTEGER
    #     3- target Id (tgtId) : INTEGER
    #     4- create time (ctime) : INTEGER
    #     5- death time (dtime) : INTEGER
    #     6- stability (stability) : TEXT
    # Note this is numbered from 1. Subtract 1 for the actual index.
    def get_source_id_from_rec( self, rec ):
        return rec[0]

    def get_source_field_id_from_rec( self, rec ):
        return rec[1]

    def get_target_id_from_rec( self, rec ):
        return rec[2]

    def get_create_time_from_rec( self, rec ):
        return rec[3]

    def get_death_time_from_rec( self, rec ):
        return rec[4]

    def get_stability_from_rec( self, rec ):
        return rec[4]

    def get_edgestate_from_rec( self, rec ):
        es = rec[5]
        return EdgeInfoReader.ES2STR[es]


class EdgeInfoFile2DB:
    def __init__( self,
                  edgeinfo_filename = "",
                  outdbfilename = "",
                  stabreader = {},
                  logger = None ):
        assert( logger != None )
        try:
            assert( os.path.isfile(edgeinfo_filename) )
        except:
            print "File not found: %s" % edgeinfo_filename
            exit(200)
        self.edgereader = EdgeInfoReader( edgeinfo_filename = edgeinfo_filename,
                                          useDB_as_source = False,
                                          stabreader = stabreader,
                                          logger = logger )
        self.edgereader.create_edgeinfo_db( outdbfilename = outdbfilename )
        self.edgereader.read_edgeinfo_file_with_stability_into_db( stabreader = stabreader )


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
class DeathGroupsReader:
    def __init__( self,
                  dgroup_file = None,
                  pickle_flag = False,
                  debugflag = False,
                  logger = None ):
        self.dgroup_file_name = dgroup_file
        # pickle_flag determines whether the source file is:
        #       * a trace file that needs cleaning
        #       * a pickle file that is already cleaned
        self.pickle_flag = pickle_flag
        # Note that while this can generalize the planned read_dgroups_file call,
        # we will still provide a different function for each and check the flag.
        # This way the intent is clearly specified.
        # ------------------------------------------------------------
        # Map of object to list of group numbers
        self.obj2group = {}
        # Map of key to group number
        self.key2group = {}
        # Map of group number to death time
        self.group2dtime = {}
        # Map of death time to groupnumber
        self.dtime2group = {}
        # Map of group number to list of objects
        self.group2list = defaultdict( list )
        # The pickle data container if it is needed:
        self.pdata = None
        # Others
        self.debugflag = debugflag
        self.logger = logger
        self._atend_gnum = None
        # DEBUG
        self.zero_dtime_total = 0
        self.regular_dtime_total = 0

    def map_key2group( self,
                       groupnum = 0,
                       keylist = [] ):
        assert( groupnum > 0 )
        k2g = self.key2group
        for k in keylist:
            if k in k2g:
                k2g[k].append( groupnum )
            else:
                k2g[k] = [ groupnum ]

    def map_obj2group( self,
                       objId = -1,
                       groupnum = 0 ):
        assert( groupnum > 0 )
        assert( objId >= 0 )
        self.obj2group[objId] = groupnum

    def get_group( self, groupnum = 0 ):
        """Returns the group as a list for groupnum."""
        return self.group2list[groupnum] if groupnum in self.group2list else []

    def get_group_number( self, objId = 0 ):
        """Returns the group number for a given object Id 'objId'"""
        return self.obj2group[objId] if objId in self.obj2group else None 

    def map_group2dtime( self,
                         groupnum = 0,
                         dtime = 0 ):
        assert( groupnum > 0 )
        if dtime <= 0:
            self.zero_dtime_total += 1
            self.logger.critical( "Zero map total: %d (vs %d)" %
                                  (self.zero_dtime_total, self.regular_dtime_total)  )
        else:
            self.regular_dtime_total += 1
        if groupnum not in self.group2dtime:
            self.group2dtime[groupnum] = dtime
            # Death time to group number mapping
            if dtime not in self.dtime2group:
                self.dtime2group[dtime] = groupnum
            else:
                if groupnum != self.dtime2group[dtime]:
                    self.logger.critical( "Same deathtime [%d] mapped to different groups [ %d != %d ]" %
                                          (dtime, groupnum, self.dtime2group[dtime]) )
                    raise RuntimeError( "Same deathtime [%d] mapped to different groups [ %d != %d ]" %
                                        (dtime, groupnum, self.dtime2group[dtime]) )
                else:
                    self.logger.error( "Double mapping of [%d] deathtime but groupnumbers are the same. Continuing." )
        # NOTE: This is made into a function because there may be
        # other things we wish to do with saving the sets of death
        # times.

    def move_group( self,
                    src = None,
                    tgt = None ):
        if src in self.group2list:
            if tgt in self.group2list:
                self.group2list[src].extend( self.group2list[tgt] )
                del self.group2list[tgt]
            else:
                self.logger.critical( "%d not found." % tgt )
        else:
            self.logger.critical( "%d not found." % src )

    def renumber_dgroups( self ):
        # Sort according to size of group. Note that this is a list and not a dictionary.
        newlist = sorted( [ (group, mylist) for group, mylist in self.group2list.iteritems() if len(mylist) > 0 ],
                          key = lambda x: len(x[1]),
                          reverse = True )
        self.group2list = {}
        g2l = self.group2list # New group2list. We don't need to save the old one here because it's
        #                       already saved in 'newlist'.
        new_o2g = {} # New obj2group
        new_g2d = {} # New group2dtime
        # new_key2g = {} new key2group but not sure if we need this
        new_dtime2g = {} # new dtime2group
        gnum = 1
        for oldgnum, mylist in newlist:
            dtime = self.group2dtime[oldgnum]
            new_dtime2g[dtime] = gnum
            new_g2d[gnum] = dtime
            for objId in mylist:
                new_o2g[objId] = gnum
            g2l[gnum] = mylist
            gnum += 1
        # Blow away the old metadata and save the new ones.
        self.obj2group = new_o2g
        self.group2dtime = new_g2d
        self.dtime2group = new_dtime2g

    # Read in the 'dirty' trace file.
    # Expects self.pickle_flag to be False
    def read_dgroup_file( self,
                          object_info_reader = None ):
        # We don't know which are the key objects. TODO TODO TODO
        if self.pickle_flag:
            raise ValueError( "Called to read the trace file, but currently configured to read in pickle file: %s." 
                              % self.dgroup_file_name )
        assert(object_info_reader != None)
        logger = self.logger
        oir = object_info_reader
        debugflag = self.debugflag
        count = 0
        start = False
        done = False
        multkey = 0
        # withkey = 0
        withoutkey = 0
        last_groupnum = 0
        with open(self.dgroup_file_name, "rb") as fptr:
            for line in fptr:
                if line.find("---------------[ CYCLES") == 0:
                    start = True if not start else False
                    if start:
                        continue
                    else:
                        done = True
                        break
                if start:
                    line = line.rstrip()
                    line = line.rstrip(",")
                    # Remove all objects that died at program end.
                    dg = [ int(x) for x in line.split(",") if not oir.died_at_end(int(x))  ]
                    if len(dg) == 0:
                        continue
                    dg = list( set(dg) )
                    for objId in dg:
                        dtime = oir.get_death_time(objId)
                        if dtime not in self.dtime2group:
                            last_groupnum += 1
                            groupnum = last_groupnum
                            self.map_obj2group( objId = objId,
                                                groupnum = groupnum )
                            self.map_group2dtime( groupnum = groupnum,
                                                  dtime = dtime )
                        else:
                            groupnum = self.dtime2group[dtime]
                            if objId in self.obj2group:
                                assert( dtime == self.group2dtime[groupnum] )
                            else:
                                self.map_obj2group( objId = objId,
                                                    groupnum = groupnum )
                    self.group2list[groupnum].extend( dg )
                    last_groupnum += 1
                    if debugflag:
                        if count % 1000 == 99:
                            sys.stdout.write("#")
                            sys.stdout.flush()
                            sys.stdout.write(str(len(line)) + " | ")
        print "----------------------------------------------------------------------"
        # Renumber according to size. Largest group first.
        # TODO: We can make this an option if we want something like oldest first.
        self.renumber_dgroups()
        #--------------------------------------------------
        # Debug print out top 5 groups --------------------
        tmpcount = 0
        for gnum, mylist in self.group2list.iteritems():
            print "Group num[ %d ]: => %d objects" % (gnum, len(mylist))
            tmpcount += 1
            if tmpcount > 4:
                break
        # END Debug ---------------------------------------
        #--------------------------------------------------
        nokey_set = set()
        for gnum, mylist in self.group2list.iteritems():
            keylist = get_key_objects( mylist, oir )
            self.map_key2group( groupnum = groupnum, keylist = keylist )
            # Debug key objects. NOTE: This may not be used for now.
            if len(keylist) > 1:
                # logger.debug( "multiple key objects: %s" % str(keylist) )
                multkey += 1
            elif len(keylist) == 0:
                tmpset = frozenset(keylist)
                if tmpset not in nokey_set:
                    # logger.debug( "NO key object in group: %s" % str(keylist) )
                    withoutkey += 1
                    nokey_set.add(tmpset)
        print "Multiple key: %d" % multkey
        print "Without key: %d" % withoutkey
        print "----------------------------------------------------------------------"

    # Read in the 'clean' pickle file.
    # Expects self.pickle_flag to be True
    def read_dgroup_pickles( self,
                             object_info_reader = None ):
        if not self.pickle_flag:
            raise ValueError( "Called to read the pickle file, but currently configured to read in trace file: %s." 
                              % self.dgroup_file_name )
        logger = self.logger
        assert(object_info_reader != None)
        oir = object_info_reader
        debugflag = self.debugflag
        with open(self.dgroup_file_name, "rb") as fptr:
            self.pdata = pickle.load(fptr)
            self.group2list = self.pdata["group2list"]
            self.group2dtime = self.pdata["group2dtime"]
            self.obj2group = self.pdata["obj2group"]
        # TODO DEBUG: raise RuntimeError("TODO: Need to implement this.")

    def create_dgroup_db( self, outdbfilename = None ):
        try:
            self.outdbconn = sqlite3.connect( outdbfilename )
        except:
            logger.critical( "Unable to open %s" % outdbfilename )
            print "Unable to open %s" % outdbfilename
            exit(1)
        conn = self.outdbconn
        conn.text_factory = str
        cur = conn.cursor()
        # ----------------------------------------------------------------------
        # Create the DGROUPS DB
        # ----------------------------------------------------------------------
        cur.execute( '''DROP TABLE IF EXISTS dgroups''' )
        # Create the database. These are the fields in order.
        # Decode as:
        # num- fullname (sqlite name) : type
        # 1- group number (gnum) : INTEGER
        # 2- group size (size) : INTEGER
        # 3- text list of object ids (objlist) : TEXT
        # 4- death time (dtime) : INTEGER
        cur.execute( """CREATE TABLE objinfo (gnum INTEGER PRIMARY KEY,
                                              size INTEGER,
                                              objlist TEXT,
                                              dtime INTEGER)""" )
        conn.execute( 'DROP INDEX IF EXISTS idx_dgroups_gnum' )

    def write_clean_dgroups_to_file( self,
                                     # TODO dbfilename = None,
                                     pickle_filename = None,
                                     group2list_filename = None,
                                     obj2group_filename = None,
                                     object_info_reader = None,
                                     save_to_db = False ):
        # TODO assert( type(dbfilename) == type("") )
        assert( type(pickle_filename) == type("") )
        assert( type(group2list_filename) == type("") )
        assert( type(obj2group_filename) == type("") )
        # This function writes out two files. The DB and this pickle file.
        # The pickle file contains the obj2group and group2dtime dictionaries.
        self.pickle_filename = pickle_filename
        #----------------------------------------------------------------------
        # Dumping the pickle first
        sys.stdout.write("Writing out the pickle file: %s\n" % pickle_filename)
        sys.stdout.flush()
        with open(pickle_filename, "wb") as fp:
            data = { "obj2group" : self.obj2group,
                     "group2dtime" : self.group2dtime,
                     "group2list" : self.group2list }
            pickle.dump( data, fp )
        # Now the group2list csv file:
        sys.stdout.write("Writing out the group2list csv file: %s\n" % group2list_filename)
        sys.stdout.flush()
        with open(group2list_filename, "wb") as fp2:
            csvwriter = csv.writer(fp2)
            header = [ "groupId", "number", "death_time", "list", ]
            csvwriter.writerow( header )
            for gnum, mylist in self.group2list.items():
                row = [ gnum ]
                row.append( len(mylist) )
                try:
                    row.append( self.group2dtime[gnum] )
                except:
                    print "ERROR:"
                    print "%d not found in new g2dtime" % gnum
                    print " - found in orig? %s" % str(gnum in self.orig_group2dtime)
                    raise ValueError( "%d not found in new g2dtime" % gnum )
                row.extend( mylist )
                csvwriter.writerow( row )
        # Now the obj2group csv file:
        sys.stdout.write("Writing out the obj2group csv file: %s\n" % obj2group_filename)
        sys.stdout.flush()
        with open(obj2group_filename, "wb") as fp3:
            csvwriter = csv.writer(fp3)
            header = [ "objId", "groupId", ]
            csvwriter.writerow( header )
            for objId, gnum in self.obj2group.items():
                row = [ objId, gnum, ]
                csvwriter.writerow( row )

    def iteritems( self ):
        return self.group2list.iteritems()

    def keys( self ):
        return self.group2list.keys()

    def items( self ):
        return self.group2list.items()

    def get_atend_group_number( self ):
        return self._atend_gnum

    def clean_deathgroups( self ):
        group2list = self.group2list
        count = 0
        for gnum in group2list.keys():
            if len(group2list[gnum]) == 0:
                del group2list[gnum]
                count += 0
            else:
                group2list[gnum] = list(set(group2list[gnum]))
        print "%d empty groups cleaned." % count

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
class ContextCountReader:
    def __init__( self,
                  context_file = None,
                  logger = None,
                  update_missing = False ):
        # TODO: Choice of loading from text file or from pickle
        #
        self.context_file_name = context_file
        # Context to counts and attribute record dictionary
        self.contextdict = {} # (funcsrc, functgt, contexttype) -> (count objects, count death groups)
        self.con_typedict = defaultdict( Counter ) # (funcsrc, functgt) -> Counter of key object types
        self.all_typedict = defaultdict( Counter ) # (funcsrc, functgt) -> Counter of all types
        self.stack_counter = Counter() # (funcsrc, functgt) -> count of stack objects
        self.logger = logger
        self.update_missing = update_missing
        self.missing_set = set([])

    def process_object_info( self,
                             object_info = None ):
        oi = object_info
        for objId, rec in oi.iterrecs():
            self.inc_count( context_pair = oi.get_death_context_record(rec),
                            objTypeId = rec[get_index( "TYPE" )],
                            by_stack = (rec[get_index("DIEDBY")] == 'S') )

    def read_context_file( self ):
        start = False
        done = False
        with get_trace_fp( self.context_file_name, self.logger ) as fp:
            for line in fp:
                line = line.rstrip()
                rowtmp = line.split(",")
                # TODO HERE TODO
                # 0 - function 1
                # 1 - function 2
                # 2 - number of objects that died at (function1, function2)
                # Note that it's either:
                # * func1 called func2
                # * func1 returned to func2
                src = rowtmp[0]
                tgt = rowtmp[1]
                cptype = rowtmp[2]
                tmpcount = int(rowtmp[3])
                self.contextdict[tuple([src, tgt, cptype])] = (tmpcount, 0)
                # Note that the 0 means it has to be updated
                # when key objects are processed later
                # TODO Don't need a src <-> mapping?
                # TODO self.srcdict[src].add( tgt )
                # TODO self.tgtdict[tgt].add( src )

    def context_iteritems( self ):
        return self.contextdict.iteritems()

    def inc_count( self,
                   context_pair = (None, None),
                   cptype = None,
                   objTypeId = 0,
                   by_stack = False ):
        cpair = context_pair
        if cpair[0] == None or cpair[1] == None:
            # Invalid context pair.
            self.logger.error("Context pair is None.")
            return False
        cdict = self.contextdict

        newkey = cpair + (cptype,)
        if newkey not in cdict:
            # Not found, initialize. Key count (second element)
            # is updated later.
            cdict[newkey] = (1, 0)
        else:
            old = cdict[newkey]
            cdict[newkey] = ((old[0] + 1), 0)
        self.all_typedict[newkey].update( [ objTypeId ] )
        if by_stack:
            self.stack_counter.update( [ newkey ] )
        return True

    def update_key_count( self,
                          context_pair = (None, None),
                          cptype = None,
                          key_count = 0 ):
        cpair = context_pair
        if cpair[0] == None or cpair[1] == None:
            self.logger.error("Context pair is None.")
            return None
        cdict = self.contextdict
        newkey = cpair + (cptype,)
        if newkey not in cdict:
            # TODO: Is this an error at all? Maybe this should be logger.debug. TODO
            self.logger.error("Context pair[ %s ] not found." % str(newkey))
            # Update the missing if we're supposed to
            if not self.update_missing:
                return False
            cdict[newkey] = (1, key_count)
            self.missing_set.add( newkey )
            return False
        else:
            self.update_key_count_no_check( newkey,
                                            key_count )
            return True

    def update_key_count_no_check( self,
                                   newkey = (None, None, None),
                                   key_count = 0 ):
        cdict = self.contextdict
        rec = cdict[newkey]
        if newkey in self.missing_set:
            # The context was missing, so we also need to update the total count.
            cdict[newkey] = ((rec[0] + 1), key_count)
        else:
            cdict[newkey] = (rec[0], key_count)

    def inc_key_count( self,
                       context_pair = (None, None),
                       objType = "NONE" ):
        cpair = context_pair
        if cpair[0] == None or cpair[1] == None:
            self.logger.error("Context pair is None.")
            return None
        cdict = self.contextdict
        result = False
        if cpair not in cdict:
            self.logger.error("Context pair[ %s ] not found." % str(cpair))
            # Update the missing if we're supposed to
            if not self.update_missing:
                return False
            cdict[cpair] = (1, 1)
            self.missing_set.add( cpair )
        else:
            self.inc_key_count_no_check( cpair )
            result = True
        self.con_typedict[cpair].update( [ objType ] )
        return result

    def inc_key_count_no_check( self,
                                cpair = (None, None) ):
        cdict = self.contextdict
        rec = cdict[cpair]
        if cpair in self.missing_set:
            cdict[cpair] = ((rec[0] + 1), (rec[1] + 1))
        else:
            cdict[cpair] = (rec[0], (rec[1] + 1))

    def fix_counts( self,
                    objectinfo = None ):
        oi = objectinfo
        cdict = self.contextdict
        for cpair in cdict.keys():
            rec = cdict[cpair]
            kcount = rec[1]
            if kcount == 0:
                cdict[cpair] = (rec[0], rec[0])
                for typeId, count in self.all_typedict[cpair].iteritems():
                    self.con_typedict[cpair][ oi.get_type_using_typeId(typeId) ] = count


    def get_top( self,
                 cpair = None,
                 num = 5 ):
        """Return the top 'num' key object types"""
        return self.con_typedict[cpair].most_common(num) if cpair != None \
            else [ "NONE" ] * num

    def get_stack_count( self,
                         cpair = None ):
        if cpair == None:
            return 0
        return self.stack_counter[cpair] if cpair in self.stack_counter else 0

    def print_out( self, numlines = 30 ):
        pass
        # TODO
        # count = 0
        # for edge, timepaid in self.edgedict.iteritems():
        #     print "(%d, %d) -> (%d, %d)" % (edge[0], edge[1], timepaid[0], timepaid[1])
        #     count += 1
        #     if numlines != 0 and count >= numlines:
        #         break

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
class SummaryReader:
    def __init__( self,
                  summary_file = None,
                  logger = None ):
        # TODO: Choice of loading from text file or from pickle
        #
        self.summary_file_name = summary_file
        self.summarydict = {}
        self.logger = logger

    def read_summary_file( self ):
        start = False
        done = False
        sdict = self.summarydict
        with get_trace_fp( self.summary_file_name, self.logger ) as fp:
            for line in fp:
                line = line.rstrip()
                if line.find("---------------[ SUMMARY INFO") == 0:
                    start = True if not start else False
                    if start:
                        continue
                    else:
                        done = True
                        break
                if start:
                    row = line.split(",")
                    # 0 - key
                    # 1 - value
                    try:
                        sdict[row[0]] = int(row[1])
                    except:
                        raise ValueError( "Unexpected line: %s" % line )
        assert(done)

    def get_final_garbology_time( self ):
        assert("final_time" in self.summarydict)
        return self.summarydict["final_time"]

    def get_final_garbology_alloc_time( self ):
        assert("final_time" in self.summarydict)
        return self.summarydict["final_time_alloc"]

    def get_number_of_objects( self ):
        assert("number_of_objects" in self.summarydict)
        return self.summarydict["number_of_objects"]

    def get_number_of_edges( self ):
        assert("number_of_edges" in self.summarydict)
        return self.summarydict["number_of_edges"]

    # died by categories in NUMBER of objects
    def get_number_died_by_stack( self ):
        assert("died_by_stack" in self.summarydict)
        return self.summarydict["died_by_stack"]

    def get_number_died_by_stack_only( self ):
        assert("died_by_stack_only" in self.summarydict)
        return self.summarydict["died_by_stack_only"]

    def get_number_died_by_heap( self ):
        assert("died_by_heap" in self.summarydict)
        return self.summarydict["died_by_heap"]

    def get_number_died_at_end( self ):
        assert("died_by_heap" in self.summarydict)
        return self.summarydict["died_at_end"]

    def get_number_died_by_stack_after_heap( self ):
        assert("died_by_stack_after_heap" in self.summarydict)
        return self.summarydict["died_by_stack_after_heap"]

    def get_number_died_by_global( self ):
        assert("died_by_global" in self.summarydict)
        return self.summarydict["died_by_global"]

    # died by categories in SIZE (bytes)
    def get_size_died_by_stack_only( self ):
        assert("died_by_stack_only" in self.summarydict)
        return self.summarydict["died_by_stack_only_size"]

    def get_size_died_by_stack_after_heap( self ):
        assert("died_by_stack_after_heap" in self.summarydict)
        return self.summarydict["died_by_stack_after_heap_size"]

    def get_size_died_by_stack( self ):
        assert("size_died_by_stack" in self.summarydict)
        return self.summarydict["size_died_by_stack"]

    def get_size_died_by_heap( self ):
        assert("size_died_by_heap" in self.summarydict)
        return self.summarydict["size_died_by_heap"]

    def get_size_died_at_end( self ):
        assert("size_died_by_heap" in self.summarydict)
        return self.summarydict["size_died_at_end"]

    # TODO def get_size_died_by_global( self ):
    # TODO     assert("died_by_global" in self.summarydict)
    # TODO     return self.summarydict["died_by_global"]

    def get_last_update_null( self ):
        assert("last_update_null" in self.summarydict)
        return self.summarydict["last_update_null"]
        
    # ======================================================================
    def get_max_live_size( self ):
        assert("max_live_size" in self.summarydict)
        return self.summarydict["max_live_size"]

    def keys( self ):
        return self.summarydict.keys()

    def items( self ):
        return self.summarydict.items()

    def __get_summarydict__( self ):
        # The __ means only call if you know what you're doing, eh?
        return self.summarydict

    def print_out( self ):
        for key, val in self.summarydict.iteritems():
            print "%s -> %d" % (key, val)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
class StabilityReader:
    def __init__( self,
                  stability_file = None,
                  logger = None ):
        self.stability_file_name = stability_file
        self.stabilitydict = {}
        self.logger = logger

    def read_stability_file( self ):
        start = False
        sdict = self.stabilitydict
        with get_trace_fp( self.stability_file_name, self.logger ) as fp:
            for line in fp:
                line = line.rstrip()
                row = line.split(",")
                # 0 - object Id
                # 1 - field Id
                # 2 - Stability type
                #       * S  = Stable
                #       * ST = Serial stable
                #       * U  = Unstable
                #       * X  = Unknown
                objId = int(row[0])
                fieldId = int(row[1])
                stype = row[2]
                if objId not in sdict:
                    sdict[objId] = { fieldId : stype }
                else:
                    if fieldId in sdict[objId]:
                        self.logger.error( "Duplicate field Id [%d]" % fieldId )
                        if sdict[objId] != stype:
                            self.logger.critical( "Mismatch [%s] != [%s]" % (sdict[objId][fieldId], stype) )
                sdict[objId][fieldId] = stype

    def iteritems( self ):
        return self.stabilitydict.iteritems()

    def keys( self ):
        return self.stabilitydict.keys()

    def items( self ):
        return self.stabilitydict.items()

    def __get_stabilitydict__( self ):
        # The __ means only call if you know what you're doing, eh?
        return self.stabilitydict

    def get_fields_dict( self, objId = None ):
        return self.__getitem__[objId]

    def __getitem__( self, objId = None ):
        return self.stabilitydict[objId] if objId in self.stabilitydict else None

    def get_stability_type( self,
                            objId = None,
                            fieldId = None ):
        if objId in self.stabilitydict:
            if fieldId in self.stabilitydict[objId]:
                return self.stabilitydict[objId][fieldId]
        # We get here if objId or fieldId aren't found
        return None

    def print_out( self ):
        for key, fdict in self.stabilitydict.iteritems():
            print "%d -> %s" % (key, str(fdict))

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
class ReferenceReader:
    def __init__( self,
                  reference_file = None,
                  useDB_as_source = False,
                  logger = None ):
        self.reference_filename = reference_file
        self.referencedict = {}
        self.logger = logger

    def read_reference_file( self ):
        start = False
        sdict = self.referencedict
        with get_trace_fp( self.reference_filename, self.logger ) as fp:
            for line in fp:
                line = line.rstrip()
                row = line.split(",")
                # 0 - object Id
                # 1 - field Id
                # 2 - Number of objects pointed at, following
                objId = int(row[0])
                fieldId = int(row[1])
                num = row[2]
                if objId not in sdict:
                    sdict[objId] = { fieldId : [ int(x) for x in row[3:] ] }
                else:
                    if fieldId in sdict[objId]:
                        self.logger.error( "Duplicate field Id [%d]" % fieldId )
                    sdict[objId][fieldId] = [ int(x) for x in row[3:] ]

    def create_refsummary_db( self, outdbfilename = None ):
        # Note that outdbconn will not be closed here.
        try:
            self.outdbconn = sqlite3.connect( outdbfilename )
        except:
            logger.critical( "Unable to open %s" % outdbfilename )
            print "Unable to open %s" % outdbfilename
            exit(1)
        conn = self.outdbconn
        conn.text_factory = str
        cur = conn.cursor()
        # ----------------------------------------------------------------------
        # Create the REF-SUMMARY DB
        # ----------------------------------------------------------------------
        cur.execute( '''DROP TABLE IF EXISTS refsummary''' )
        # Create the database. These are the fields in order.
        # Decode as:
        # num- fullname (sqlite name) : type
        # 1- object Id (objid) : INTEGER
        # 2- field Id (fieldid) : INTEGER
        # 3- number of objects pointed at (numtarget) : INTEGER
        # 4- list of object Ids (objlist) : TEXT
        #    NOTE: This is in TEXT so that we can put all in one field.
        #          It is up to the user program to separate the simple
        #          comma separated object list.
        cur.execute( """CREATE TABLE refsummary (objid INTEGER KEY,
                                                 fieldid INTEGER,
                                                 numtarget INTEGER,
                                                 objlist TEXT)""" )
        conn.execute( 'DROP INDEX IF EXISTS idx_refsummary_objid' )

    def read_refsummary_into_db( self ):
        # Declare our generator
        # ----------------------------------------------------------------------
        def row_generator():
            with get_trace_fp( self.reference_filename, self.logger ) as fp:
                for line in fp:
                    orig = line
                    line = line.rstrip()
                    row = line.split(",")
                    # 1- object Id (objid) : INTEGER
                    # 2- field Id (fieldid) : INTEGER
                    # 3- number of objects pointed at (numtarget) : INTEGER
                    # 4- list of object Ids (objlist) : TEXT
                    result = row[0:3]
                    try:
                        assert(len(row) >= 3) # TODO DEBUG
                    except:
                        print orig
                        print str(row)
                        exit(100)
                    result.append( str(row[3:]) )
                    result = tuple(result)
                    # TODO DEBUG: print "XXX:", str(result)
                    yield result

        cur = self.outdbconn.cursor()
        cur.executemany( "INSERT INTO refsummary VALUES (?,?,?,?)", row_generator() )
        cur.execute( 'CREATE UNIQUE INDEX idx_refsummary_objid ON refsummary (objid)' )
        self.outdbconn.commit()
        self.outdbconn.close()


    def iteritems( self ):
        return self.referencedict.iteritems()

    def keys( self ):
        return self.referencedict.keys()

    def items( self ):
        return self.referencedict.items()

    def __get_referencedict__( self ):
        # The __ means only call if you know what you're doing, eh?
        return self.referencedict

    def __getitem__( self, key ):
        """Returns the list at key where key is a reference - (object Id, field Id) tuple."""
        objId = key[0]
        fieldId = key[1]
        rdict = self.referencedict
        if (objId in rdict):
            if (fieldId in rdict[objId]):
                return rdict[objId][fieldId]
            else:
                raise ValueError("Field Id[%s] not found." % str(fieldId))
        else:
            raise ValueError("Object Id[%s] not found." % str(objId))
        assert(False) # Shouldn't reach here.

    def print_out( self ):
        for key, fdict in self.referencedict.iteritems():
            print "%d -> %s" % (key, str(fdict))

class ReferenceFile2DB:
    def __init__( self,
                  reference_filename = "",
                  outdbfilename = "",
                  logger = None ):
        assert( logger != None )
        assert( os.path.isfile(reference_filename) )
        self.refreader = ReferenceReader( reference_file = reference_filename,
                                          useDB_as_source = False,
                                          logger = logger )
        self.refreader.create_refsummary_db( outdbfilename = outdbfilename )
        self.refreader.read_refsummary_into_db()

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
class ReverseRefReader:
    def __init__( self,
                  reverseref_file = None,
                  logger = None ):
        self.reverseref_filename = reverseref_file
        self.reverserefdict = {}
        self.logger = logger

    def read_reverseref_file( self ):
        start = False
        sdict = self.reverserefdict
        with get_trace_fp( self.reverseref_filename, self.logger ) as fp:
            for line in fp:
                line = line.rstrip()
                line = line.replace("(","")
                line = line.replace(")","")
                row = line.split(",")
                # 0 - object Id
                # 1 - Number of references pointing at this object
                # The original text list of pairs looks like this:
                # (0,1),(2,3),(4,5)
                # So we remove the parentheses, and get a list of pairs.
                # 0, 1, 2, 3, 4, 5
                objId = int(row[0])
                num = int(row[1])
                if objId not in sdict:
                    it = iter( [ int(x) for x in row[3:] ] )
                    sdict[objId] = zip(it, it)
                else:
                    self.logger.critical( "Duplicate object Id [%d]" % fieldId )
                    print "Duplicate object Id [%d]" % fieldId
                    print "Exiting."
                    exit(1)
                # TODO: Parse the (objId, fieldId) reference pairs
                # TODO

    def iteritems( self ):
        return self.reverserefdict.iteritems()

    def keys( self ):
        return self.reverserefdict.keys()

    def items( self ):
        return self.reverserefdict.items()

    def __get_reverserefdict__( self ):
        # The __ means only call if you know what you're doing, eh?
        return self.reverserefdict

    def print_out( self ):
        for key, fdict in self.referencedict.iteritems():
            print "%d -> %s" % (key, str(fdict))

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Example:
#    #     git version: 7c8d143510e9ee00303a8345acfbac5343305d57
#    #     build date : Mon Oct  3 22:10:36 EDT 2016
#    ---------------[ START ]-----------------------------------------------------------
#    Read names file...
#    Start trace...
#    Update time: 100000 | Method time: TODO | Alloc time: 928688
#    Update time: 100000 | Method time: TODO | Alloc time: 928688
#    Update time: 100000 | Method time: TODO | Alloc time: 928688
#    main_time:103866
#    ---------------[ DONE ]------------------------------------------------------------
#    #     git version: 7c8d143510e9ee00303a8345acfbac5343305d57
#    #     build date : Mon Oct  3 22:10:36 EDT 2016
def read_main_file( main_file_name = "",
                    logger = None ):
    main_time = -1
    alloc_time = -1
    with get_trace_fp( main_file_name, logger ) as fp:
        for line in fp:
            line = line.rstrip()
            if line.find("main_time:") == 0:
                main_time = int( line.replace("main_time:", "") )
            elif line.find("alloc_time:") == 0:
                alloc_time = int( line.replace("alloc_time:", "") )
    assert( (main_time >= 0) and (alloc_time >=0) )
    return { "main_time" : main_time,
             "alloc_time" : alloc_time }


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
class NamesReader:
    def __init__( self,
                  names_file = None,
                  logger = None ):
        self.names_filename = names_file
        self.namesdict = {}
        self.classdict = {}
        self.logger = logger

    def read_names_file( self ):
        start = False
        fdict = self.namesdict
        cdict = self.classdict
        with get_trace_fp( self.names_filename, self.logger ) as fp:
            for line in fp:
                line = line.rstrip()
                row = line.split(" ")
                # 0 - Entity type [F,C,E,I,S]
                # If entity == "F":
                #     1 - field kind
                #           * S = static
                #           * I = instance
                #     2 - field Id (in hex 0xNNN format)
                #         NOTE: Field ids are globally unique so we can use this
                #               to index the field dictionary (namesdict)
                #     3 - field name
                #     4 - class Id (in hex 0xNNN format)
                #     5 - class name
                #     6 - field target type
                # else if entity == "C":
                #     TODO
                # else if entity == "E":
                #     TODO
                # else if entity == "I":
                #     TODO
                # else if entity == "S":
                #     TODO
                recordType = row[0]
                if recordType == "F":
                    # print ".",
                    fieldKind = row[1]
                    fieldId = hex2dec(row[2])
                    fieldName = row[3]
                    classId = hex2dec(row[4])
                    className = row[5]
                    if classId not in cdict:
                        cdict[classId] = className
                    fieldTgtType = row[6]
                    if fieldId not in fdict:
                        fdict[fieldId] = { "fieldKind" : fieldKind,
                                           "fieldName" : fieldName, 
                                           "classId" : classId,
                                           "fieldTgtType" : fieldTgtType, }
                    else:
                        self.logger.error( "Duplicate field Id [%d]" % fieldId )
                        # TODO: Check to see that it matches up?
                        assert(False) # Bail out for now. This shouldn't happen though. TODO
                else:
                    # print recordType,
                    pass
                    # Ignore the rest for now
                    # TODO TODO TODO

    def iteritems( self ):
        return self.namesdict.iteritems()

    def keys( self ):
        return self.namesdict.keys()

    def items( self ):
        return self.namesdict.items()

    def __get_namesdict__( self ):
        # The __ means only call if you know what you're doing, eh?
        return self.namesdict

    def get_fields_dict( self, fieldId = None ):
        return self.__getitem__[fieldId]

    def __getitem__( self, fieldId = None ):
        return self.namesdict[fieldId] if fieldId in self.namesdict else None

    def print_out( self ):
        for key, fdict in self.namesdict.iteritems():
            print "%d -> %s" % (key, str(fdict))

    def get_field_name( self, fieldId = None ):
        rec = self[fieldId]
        return rec["fieldName"] if rec != None else "None"

    def get_class_id( self, fieldId = None ):
        rec = self[fieldId]
        return rec["classId"] if rec != None else None

    def get_field_target_type( self, fieldId = None ):
        rec = self[fieldId]
        return rec["fieldTgtType"] if rec != None else "None"


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#
#  PRIVATE FUNCTIONS
#

def setup_logger( targetdir = ".",
                  filename = "garbology.log",
                  logger_name = 'garbology',
                  debugflag = 0 ):
    # Set up main logger
    logger = logging.getLogger( logger_name )
    formatter = logging.Formatter( '[%(funcName)s] : %(message)s' )
    filehandler = logging.FileHandler( os.path.join( targetdir, filename ) , 'w' )
    if debugflag:
        logger.setLevel( logging.DEBUG )
        filehandler.setLevel( logging.DEBUG )
    else:
        filehandler.setLevel( logging.ERROR )
        logger.setLevel( logging.ERROR )
    filehandler.setFormatter( formatter )
    logger.addHandler( filehandler )
    return logger

def get_trace_fp( tracefile = None,
                  logger = None ):
    if not os.path.isfile( tracefile ) and not os.path.islink( tracefile ):
        # File does not exist
        logger.error( "Unable to open %s" % str(tracefile) )
        print "Unable to open %s" % str(tracefile)
        exit(21)
    bz2re = re.compile( "(.*)\.bz2$", re.IGNORECASE )
    gzre = re.compile( "(.*)\.gz$", re.IGNORECASE )
    bz2match = bz2re.search( tracefile )
    gzmatch = gzre.search( tracefile )
    if bz2match:
        # bzip2 file
        fp = subprocess.Popen( [ "bzcat", tracefile ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    elif gzmatch:
        # gz file
        fp = subprocess.Popen( [ "zcat", tracefile ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    else:
        fp = open( tracefile, "r")
    return fp

#
# Main processing
#

def create_work_directory( work_dir, logger = None, interactive = False ):
    os.chdir( work_dir )
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    if os.path.isfile(today):
        print "Can not create %s as directory." % today
        exit(11)
    if not os.path.isdir( today ):
        os.mkdir( today )
    else:
        print "WARNING: %s directory exists." % today
        logger.warning( "WARNING: %s directory exists." % today )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    return today

def main_process( logger = None,
                  gconfig = None,
                  checkFiles_flag = True,
                  debugflag = False,
                  verbose = False ):
    global pp
    gconfig.print_all_config( pp )
    if checkFiles_flag:
        gconfig.verify_all_exist( printflag = verbose )
    print "===========[ DONE ]==================================================="

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "config",
                         help = "Specify configuration filename." )
    parser.add_argument( "benchmark",
                         help = "Perform action for this benchmark.",
                         action = "store_true" )
    parser.add_argument( "--check-files",
                         dest = "check_files",
                         help = "Check if files exist.",
                         action = "store_true" )
    parser.add_argument( "--verbose",
                         dest = "verbose",
                         help = "Enable verbose output.",
                         action = "store_true" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "garbology.log",
                         check_files = False,
                         verbose = False,
                         config = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    assert( args.config != None )
    assert( os.path.isfile( args.config ) )
    gconfig = GarbologyConfig( args.config )
    checkFiles_flag = args.check_files
    debugflag = gconfig.global_cfg["debug"]
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = debugflag )
    #
    # Main processing
    #
    return main_process( logger = logger,
                         gconfig = gconfig,
                         checkFiles_flag = checkFiles_flag,
                         benchmark = benchmark,
                         debugflag = debugflag,
                         verbose = args.verbose )

__all__ = [ "EdgeInfoReader", "GarbologyConfig", "ObjectInfoReader",
            "ContextCountReader", "ReferenceReader", "ReverseRefReader",
            "StabilityReader", "ObjectInfoFile2DB", "EdgeInfoFile2DB",
            "ReferenceFile2DB",
            "is_key_object", "get_index", "is_stable", "read_main_file",
            "EdgeInfoTable", "EdgeInfoKeyField", "get_edgeinfo_db_filename",
             ]

if __name__ == "__main__":
    main()

#================================================================================
# Unused code
# TODO class EdgeInfoCache( collections.Mapping ):
# TODO     def __init__( self,
# TODO                   tgtpath = None,
# TODO                   cachesize = 5000000,
# TODO                   logger = None ):
# TODO         self.conn = sqlite3.connect( tgtpath )
# TODO         assert( table != None )
# TODO         self.table = "edgeinfo"
# TODO         self.count = None
# TODO         self.keyfield1 = "srcid"
# TODO         self.keyfield2 = "srcfield"
# TODO         self.keyfield3 = "tgtid"
# TODO         self.keyindex1 = 0
# TODO         self.keyindex2 = 1
# TODO         self.keyindex3 = 2
# TODO         # We save all the keys. Note that once we have all the keys, then we don't
# TODO         # need to ask the DB again since we don't allow writes. This will be a
# TODO         # set of tuples.
# TODO         self.keyset = set()
# TODO         # We want to know if we have all the keys.
# TODO         self.have_all_keys = False
# TODO         # We use an LRU cache to store results.
# TODO         self.lru = pylru.lrucache( size = cachesize )
# TODO         # NOTE: We assume that the keyfield is always the first field in the record
# TODO         #       tuple.
# TODO         self.logger = logger
# TODO 
# TODO     def __iter__( self ):
# TODO         if self.have_all_keys:
# TODO             for key in self.keyset:
# TODO                 yield key
# TODO         else:
# TODO             cur = self.conn.cursor()
# TODO             cur.execute( "SELECT * FROM %s" % self.table )
# TODO             for key in self.keyset:
# TODO                 yield key
# TODO             ind1 = self.keyindex1
# TODO             ind2 = self.keyindex2
# TODO             ind3 = self.keyindex3
# TODO             indrest = ind3 + 1
# TODO             while True:
# TODO                 reclist = cur.fetchmany()
# TODO                 if len(reclist) > 0:
# TODO                     for rec in reclist:
# TODO                         key = ( rec[ind1], rec[ind2], rec[ind3] )
# TODO                         if key not in self.lru:
# TODO                             # __iter__ only needs to return the key. But since
# TODO                             # we already have the record, we store it in the cache.
# TODO                             # The likelihood that the user will ask for the the record is high.
# TODO                             self.lru[key] = rec[indrest:]
# TODO                         if key in self.keyset:
# TODO                             # If we have already returned the key, just go to the next one.
# TODO                             continue
# TODO                         self.keyset.add( key )
# TODO                         yield key
# TODO                 else:
# TODO                     # This is one of the times when we know we have all the keys.
# TODO                     self.have_all_keys = True
# TODO                     raise StopIteration
# TODO 
# TODO     def iteritems( self ):
# TODO         cur = self.conn.cursor()
# TODO         cur.execute( "SELECT * FROM %s" % self.table )
# TODO         ind1 = self.keyindex1
# TODO         ind2 = self.keyindex2
# TODO         ind3 = self.keyindex3
# TODO         indrest = ind3 + 1
# TODO         while True:
# TODO             reclist = cur.fetchmany()
# TODO             if len(reclist) > 0:
# TODO                 for rec in reclist:
# TODO                     key = ( rec[ind1], rec[ind2], rec[ind3] )
# TODO                     if key not in self.lru:
# TODO                         self.lru[key] = rec[indrest:]
# TODO                     self.keyset.add( key ) # Might as well add to the key set
# TODO                     yield (key, rec[indrest:])
# TODO             else:
# TODO                 # This is one of the times when we know we have all the keys.
# TODO                 self.have_all_keys = True
# TODO                 raise StopIteration
# TODO 
# TODO     def keys( self ):
# TODO         if self.have_all_keys:
# TODO             # Return a copy
# TODO             return set(self.keyset)
# TODO         else:
# TODO             cur = self.conn.cursor()
# TODO             cur.execute( "SELECT %s FROM %s" % (self.keyfield, self.table) )
# TODO             mykeyset = set()
# TODO             while True:
# TODO                 keylist = cur.fetchmany()
# TODO                 if len(keylist) > 0:
# TODO                     mykeyset.update( [ x[0] for x in keylist ] )
# TODO                 else:
# TODO                     break
# TODO             # Result should be a list.
# TODO             result = list(mykeyset)
# TODO             # This debug check happens only once, so it's ok to do it.
# TODO             for x in result:
# TODO                 try:
# TODO                     assert(type(x) == type(1))
# TODO                 except:
# TODO                     print "kEY ERROR:"
# TODO                     print "x:", x
# TODO                     exit(100)
# TODO             self.keyset = mykeyset
# TODO             self.have_all_keys = True
# TODO             return result
# TODO 
# TODO     def __contains__( self, item ):
# TODO         if item in self.lru:
# TODO             return True
# TODO         else:
# TODO             cur = self.conn.cursor()
# TODO             cmd = "SELECT * FROM %s WHERE %s=%s" % ( self.table, self.keyfield, str(item) )
# TODO             # self.logger.debug( "CMD: %s" % cmd )
# TODO             cur.execute( cmd )
# TODO             retlist = cur.fetchmany()
# TODO             if len(retlist) != 1:
# TODO                 return False
# TODO             rec = retlist[0]
# TODO             self.lru[item] = rec[1:]
# TODO             return True
# TODO 
# TODO     def __len__(self):
# TODO         cur = self.conn.cursor()
# TODO         if self.count == None:
# TODO             cur.execute( "SELECT Count(*) FROM %s" % self.table )
# TODO         self.count = cur.fetchone()
# TODO         # DEBUG: print "%s : %s" %(str(self.count), str(type(self.count)))
# TODO         return self.count[0]
# TODO 
# TODO     def __getitem__(self, key):
# TODO         if key in self.lru:
# TODO             return self.lru[key]
# TODO         else:
# TODO             cur = self.conn.cursor()
# TODO             cur.execute( "select * from %s where %s=%s" %
# TODO                          ( self.table, self.keyfield, str(key) ) )
# TODO             retlist = cur.fetchmany()
# TODO             if len(retlist) < 1:
# TODO                 raise KeyError( "%s not found" % str(key ) )
# TODO             elif len(retlist) > 1:
# TODO                 pass
# TODO                 # todo: need to log an error here. or at least a warning.
# TODO             rec = retlist[0]
# TODO             key = rec[0]
# TODO             self.lru[key] = rec[1:]
# TODO             return rec[1:]
# TODO 
# TODO     def getitem_from_table(self, key, mytable, mykeyfield):
# TODO         cur = self.conn.cursor()
# TODO         cur.execute( "select * from %s where %s=%s" %
# TODO                      ( mytable, mykeyfield, str(key) ) )
# TODO         retlist = cur.fetchmany()
# TODO         if len(retlist) < 1:
# TODO             raise KeyError( "%s not found" % str(key ) )
# TODO         elif len(retlist) > 1:
# TODO             pass
# TODO             # todo: need to log an error here. or at least a warning.
# TODO         rec = retlist[0]
# TODO         return rec
# TODO 
# TODO     def close( self ):
# TODO         if self.conn != None:
# TODO             self.conn.close()
# TODO 
# TODO     # This is a Read-Only cache. The following are therefore not implemented.  def __additem__(self, key):
# TODO         raise NotImplemented
# TODO 
# TODO     def __setitem__(self, key, value):
# TODO         raise NotImplemented
# TODO 
# TODO     def __delitem__(self, key):
# TODO         raise NotImplemented
# TODO 
