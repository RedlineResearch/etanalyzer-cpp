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
from functools import reduce

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
                  objinfo_file = None,
                  logger = None ):
        # TODO: Choice of loading from text file or from pickle
        self.objinfo_file_name = objinfo_file
        self.objdict = {}
        self.typedict = {}
        self.rev_typedict = {}
        self.keyset = set([])
        self.alloc_age_list = []
        self.methup_age_list = []
        self.logger = logger
        # These are computed later on and are simply set to None here
        self.objId2stable_gnum = {}
        self.objId2death_gnum = {}
        # The following is the combined stable+death group numbers.
        #    We have it returning None by default for anything that hasn't
        #    been assigned yet.
        self.objId2combined_gnum = defaultdict( lambda: None )

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

    def read_objinfo_file( self, shared_list = None ):
        start = False
        count = 0
        done = False
        object_info = self.objdict
        with get_trace_fp( self.objinfo_file_name, self.logger ) as fp:
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
                    # 0 - allocation time
                    # 1 - death time
                    # 2 - size
                    objId = int(rec[ get_raw_index("OBJID") ])
                    # IMPORTANT: Any changes here, means you have to make the
                    # corresponding change up in function 'get_index'
                    # The price of admission for a dynamically typed language.
                    row = [ int(rec[ get_raw_index("ATIME") ]),
                            int(rec[ get_raw_index("DTIME") ]),
                            int(rec[ get_raw_index("SIZE") ]),
                            self.get_typeId( rec[ get_raw_index("TYPE") ] ),
                            rec[ get_raw_index("DIEDBY") ],
                            rec[ get_raw_index("LASTUP") ],
                            rec[ get_raw_index("STATTR") ],
                            rec[ get_raw_index("GARBTYPE") ],
                            rec[ get_raw_index("CONTEXT1") ],
                            rec[ get_raw_index("CONTEXT2") ],
                            rec[ get_raw_index("DEATH_CONTEXT_TYPE") ],
                            rec[ get_raw_index("ALLOC_CONTEXT1") ],
                            rec[ get_raw_index("ALLOC_CONTEXT2") ],
                            rec[ get_raw_index("ALLOC_CONTEXT_TYPE") ],
                            int(rec[ get_raw_index("ATIME_ALLOC") ]),
                            int(rec[ get_raw_index("DTIME_ALLOC") ]),
                            rec[ get_raw_index("ALLOCSITE") ],
                            ]
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
                if ( (shared_list != None) and
                     (count % 2500 >= 2499) ):
                    shared_list.append( count )
        assert(done)

    def get_typeId( self, mytype ):
        typedict = self.typedict
        rev_typedict = self.rev_typedict
        if mytype in typedict:
            return typedict[mytype]
        else:
            lastkey = len(typedict.keys())
            typedict[mytype] = lastkey + 1
            rev_typedict[lastkey + 1] = mytype
            return lastkey + 1

    def died_at_end( self, objId ):
        return (self.objdict[objId][get_index("DIEDBY")] == "E") if (objId in self.objdict) \
            else False

    def get_death_cause( self, objId ):
        rec = self.get_record(objId)
        return self.get_death_cause_using_record(rec)

    def get_death_cause_using_record( self, rec = None ):
        return rec[get_index("DIEDBY")] if (rec != None) \
            else "NONE"

    def iteritems( self ):
        """Returns (objId, rec) where rec is the record for the object with that object id."""
        return self.objdict.iteritems()

    def iterrecs( self ):
        """Returns (objId, rec) where rec is the record for the object with that object id."""
        # TODO Is there any real difference between iteritems and iterrecs? TODO
        odict = self.objdict
        keys = odict.keys()
        for objId in keys:
            yield (objId, odict[objId])

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

    def get_type( self, objId = 0 ):
        rec = self.get_record(objId)
        typeId = rec[ get_index("TYPE") ] if rec != None else None
        if typeId != None:
            return self.rev_typedict[typeId]
        else:
            return "NONE"

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
        first = rec[ get_index("CONTEXT1") ] if rec != None else "NONE"
        second = rec[ get_index("CONTEXT2") ] if rec != None else "NONE"
        return (first, second)

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

    def set_combined_sd_group_number( self,
                                      objId = None,
                                      gnum = None ):
        self.objId2combined_gnum[objId] = gnum

    def get_stable_group_number( self,
                                 objId = None ):
        return self.objId2stable_gnum[objId] if objId in self.objId2stable_gnum else None

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

# ----------------------------------------------------------------------------- 
# ----------------------------------------------------------------------------- 
class EdgeInfoReader:
    def __init__( self,
                  edgeinfo_file = None,
                  logger = None ):
        # TODO: Choice of loading from text file or from pickle
        # 
        self.edgeinfo_file_name = edgeinfo_file
        # Edge dictionary
        self.edgedict = {} # (src, tgt) -> (create time, death time)
        # Source to target object dictionary
        self.srcdict = defaultdict( set ) # src -> set of tgts
        # Target to incoming source object dictionary
        self.tgtdict = defaultdict( set ) # tgt -> set of srcs
        # Target object to record of last edge
        self.lastedge = {} # tgt -> (list of lastedges, death time)
        self.logger = logger

    def read_edgeinfo_file( self ):
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
                    row = [ int(x) for x in rowtmp ]
                    src = row[0]
                    tgt = row[1]
                    timepair = tuple(row[2:])
                    dtime = row[3]
                    fieldId = row[4] 
                    self.edgedict[tuple([src, fieldId, tgt])] = { "tp" : timepair, 
                                                                  "s" : "X" }  # X means unknown
                    self.srcdict[src].add( tgt )
                    self.tgtdict[tgt].add( src )
                    self.update_last_edges( src = src,
                                            tgt = tgt,
                                            deathtime = dtime )
        assert(done)

    def update_stability( self,
                          stabreader = {} ):
        raise RuntimeError("TODO: This needs to be implemented.")

    def read_edgeinfo_file_with_stability( self,
                                           stabreader = {} ):
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
                    timepair = tuple(row[2:])
                    dtime = row[3]
                    fieldId = row[4] 
                    try:
                        stability = sb[src][fieldId]
                    except:
                        stability = "X" # X means unknown
                    self.edgedict[tuple([src, fieldId, tgt])] = { "tp" : timepair, 
                                                                  "s" : stability }
                    self.srcdict[src].add( tgt )
                    self.tgtdict[tgt].add( src )
                    self.update_last_edges( src = src,
                                            tgt = tgt,
                                            deathtime = dtime )
        assert(done)

    def get_targets( self, src = 0 ):
        return self.srcdict[src] if (src in self.srcdict) else []

    def get_sources( self, tgt = 0 ):
        return self.tgtdict[tgt] if (tgt in self.tgtdict) else []

    def edgedict_iteritems( self ):
        return self.edgedict.iteritems()

    def srcdict_iteritems( self ):
        return self.srcdict.iteritems()

    def tgtdict_iteritems( self ):
        return self.tgtdict.iteritems()

    def lastedge_iteritems( self ):
        return self.lastedge.iteritems()

    def print_out( self, numlines = 30 ):
        count = 0
        for edge, rec in self.edgedict.iteritems():
            timepair = rec["tp"]
            print "(%d[ %d ], %d) -> (%d, %d)" % (edge[0], edge[1], edge[2], timepair[0], timepair[1])
            count += 1
            if numlines != 0 and count >= numlines:
                break

    def get_edge_times( self, edge = None ):
        """The parameter 'edge' is a tuple (src object, field Id, tgt object)."""
        if edge in self.edgedict:
            return self.edgedict[ edge ]
        else:
            return (None, None)

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
    
    def get_last_edge_record( self, tgtId = None ):
        return self.lastedge[tgtId] if tgtId in self.lastedge else None

# ----------------------------------------------------------------------------- 
# ----------------------------------------------------------------------------- 
class DeathGroupsReader:
    def __init__( self,
                  dgroup_file = None,
                  debugflag = False,
                  logger = None ):
        self.dgroup_file_name = dgroup_file
        # Map of object to list of group numbers
        self.obj2group = {}
        # Map of key to group number
        self.key2group = {}
        # Map of key to death time 
        self.group2dtime = {}
        # Map of group number to list of objects
        self.group2list= {}
        self.debugflag = debugflag
        self.logger = logger
        self._atend_gnum = None
        
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
                       groupnum = 0,
                       groupset = set([]) ):
        assert( groupnum > 0 )
        ogroup = self.obj2group
        for obj in groupset:
            if obj in ogroup:
                ogroup[obj].add( groupnum )
            else:
                ogroup[obj] = set([ groupnum ])

    def get_group( self, groupnum = 0 ):
        """Returns the group as a list for groupnum."""
        return self.group2list[groupnum] if groupnum in self.group2list else []

    def get_group_number( self, objId = 0 ):
        """Returns the group number for a given object Id 'objId'"""
        def _group_len(gnum):
            return len(self.group2list[gnum])
        glist = list(self.obj2group[objId]) if (objId in self.obj2group) else []
        if len(glist) > 1:
            gmax = max( glist, key = _group_len )
            self.logger.critical( "Multiple group numbers for objId[ %d ]: using %d"
                                  % (objId, gmax) )
            return gmax
        return glist[0] if len(glist) == 1 else None

    def map_group2dtime( self,
                         groupnum = 0,
                         dtime = 0 ):
        assert( groupnum > 0 )
        self.group2dtime[groupnum] = dtime
        # NOTE: This is made into a function because there may be
        # other things we wish to do with saving the sets of death
        # times.

    def clean_dtimes( self,
                      objreader = {} ):
        # Rename into shorter aliases
        o2g = self.obj2group
        oir = objreader
        counter = Counter()
        dtimes = {}
        newgroup = defaultdict(set)
        dtime2group = {}
        for gnum in self.group2list.keys():
            origdtime = self.group2dtime[gnum]
            dtimes[gnum] = set( [ oir.get_death_time(x) for x in self.group2list[gnum] ] )
            dtime2group[origdtime] = gnum
            if len(dtimes[gnum]) > 1:
                counter[len(dtimes[gnum])] += 1
                # Clean up aisle greater than 1. (Bad joke).
                # We will let the original group number keep the dtime that has been
                # assigned in group2dtime.
                for objId in self.group2list[gnum]:
                    dt = oir.get_death_time(objId)
                    if dt != origdtime:
                        # We need to either assign to an existing group that has
                        # the same dtime, or create a new group. But we'll do that later
                        newgroup[dt].add(objId)
                        # 1- Remove from group2list
                        self.group2list[gnum].remove( objId )
                        # 2- Remove from obj2group
                        if objId in self.obj2group:
                            del self.obj2group[objId]
                    else:
                        # Object belongs to original group.
                        # There's no need to move groups.
                        pass
                # What needs to be adjusted if we split the group?
                # group2dtime
                # group2list
                # obj2group
            else:
                counter[1] += 1
                newdtime = list(dtimes[gnum])[0]
                if newdtime != origdtime:
                    print "ERROR: Group num[ %d ] dtimes do not match  %d != %d" % \
                        (gnum, newdtime, origdtime)
                    self.logger.error( "Group num[ %d ] dtimes do not match  %d != %d" %
                                       (gnum, newdtime, origdtime) )
        # Remove empty lists in group2list
        for gnum in self.group2list.keys():
            if len(self.group2list[gnum]) == 0:
                self.group2list[gnum].remove(gnum)
                print "DEBUG: Group number %d removed" % gnum
                self.logger.error( "Group number %d removed" % gnum )
        # Get the largest groupnumber in group2list
        last_gnum = max( self.group2list.keys() ) 
        # Get the newgroup dictionary and reassign if needed
        for dt, myset in newgroup.iteritems():
            # Get the group number based on death time
            if dt in dtime2group:
                gnum = dtime2group[dt]
            else:
                gnum = last_gnum + 1
                last_gnum = gnum
                dtime2group[dt] = gnum
            # Add 'myset' to group2list
            if gnum in self.group2list:
                self.group2list[gnum].extend(list(myset))
            else:
                self.group2list[gnum] = list(myset)
            # Go through the new additions from 'myset' and set the proper obj2group
            for objId in self.group2list[gnum]:
                self.obj2group[objId] = [ gnum ]
            # Set the proper death time for the group
            self.group2dtime[gnum] = dt
        # DEBUG statements. Keeping it here just in case. -RLV
        # print "=======[ CLEAN DEBUG ]=========================================================="
        # pp.pprint(counter)
        # print "--------------------------------------------------------------------------------"
        # pp.pprint(newgroup)
        # print "NEW MAX", last_gnum
        # print "=======[ END CLEAN DEBUG ]======================================================"
        return (len(counter) == 1)

    def merge_groups_with_same_dtime( self,
                                      objreader = {},
                                      verify = False ):
        # Rename into shorter aliases
        oir = objreader
        g2d = self.group2dtime
        counter = Counter()
        dtime2group = defaultdict(set)
        for gnum in g2d.keys():
            dt = self.group2dtime[gnum]
            dtime2group[dt].add(gnum)
        # Start with known groups
        new_dtime2group = {}
        for dtime, gset in dtime2group.iteritems():
            if len(gset) > 1:
                # Merge into the lower group number
                # Sort the set into increasing group numbers
                glist = sorted( list(gset) )
                newgnum = glist[0]
                for other in glist[1:]:
                    # Save the list
                    otherlist = self.group2list[other] 
                    # Remove it
                    del self.group2list[other]
                    # Add to the target group
                    self.group2list[newgnum].extend(otherlist)
                    # Remove old group number
                    del self.group2dtime[other]
                    # Update the obj2group map
                    for objId in otherlist:
                        self.obj2group[objId] = set([ newgnum ])
            else:
                new_dtime2group[dtime] = list(gset)[0]
        dtime2group = new_dtime2group
        # Next clean the ones who don't belong to a group. Most of these (all?)
        # are "died at end of program" objects.
        # Get the largest known group number and start from there
        last_gnum = max( self.group2list.keys() )
        # Save a group number for all objects that died at end
        atend_gnum = last_gnum + 1
        self._atend_gnum = atend_gnum
        last_gnum = atend_gnum
        for objId in oir.keys():
            if objId not in self.obj2group:
                # A "no death group" object
                dt = oir.get_death_time(objId)
                if dt in dtime2group:
                    # Known death time. Add it there
                    gnum = dtime2group[dt]
                    self.group2list[gnum].append(objId)
                    self.obj2group[objId] = [ gnum ]
                    self.logger.error( "Adding object [%d] to group [%d]" % (objId, gnum) )
                else:
                    # Alert: new death time. Create a new group.
                    if oir.died_at_end(objId):
                        # Adding to DIED AT END group
                        dtime2group[dt] = atend_gnum
                        if atend_gnum in self.group2list:
                            self.group2list[atend_gnum].append( objId )
                        else:
                            self.group2list[atend_gnum] = [ objId ]
                        self.obj2group[objId] = [ atend_gnum ]
                        self.logger.error( "Adding object [%d] to AT END group [%d]" % (objId, atend_gnum) )
                    else:
                        # Add to a new group
                        gnum = last_gnum + 1
                        last_gnum = gnum
                        dtime2group[dt] = gnum
                        self.group2list[gnum] = [ objId ]
                        self.obj2group[objId] = [ gnum ]
                        self.logger.error( "Adding object [%d] to group [%d]" % (objId, gnum) )
        # Do we need to verify?
        if verify:
            dtime2group = defaultdict(set)
            for gnum in g2d.keys():
                dt = self.group2dtime[gnum]
                dtime2group[dt].add(gnum)
            errorflag = False
            for dtime, gnumset in dtime2group.iteritems():
                if len(gnumset) > 1:
                    errorflag = True
                    print "Merge NOT successful -> group [%d] => %d" % (gnumset, len(gnumset))
                    self.logger.critical( "Merge NOT successful -> group [%d] => %d" % (gnumset, len(gnumset)) )
            if errorflag:
                print "EXITING."
                exit(1)

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

    def read_dgroup_file( self,
                          object_info_reader = None ):
        # We don't know which are the key objects. TODO TODO TODO
        assert(object_info_reader != None)
        with open(self.dgroup_file_name, "rb") as fptr:
            count = 0
            dupeset = set([])
            start = False
            done = False
            debugflag = self.debugflag
            seenset = set([])
            multkey = 0
            # withkey = 0
            withoutkey = 0
            groupnum = 1
            logger = self.logger
            oir = object_info_reader
            dtind = get_index("DTIME")
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
                    dtimes = list( set( [ oir.get_death_time(x) for x in dg ] ) )
                    if (len(dtimes) > 1):
                         # TODO: Should we split into groups according to death times?
                         logger.debug( "Multiple death times: %s" % str(dtimes) )
                    # dglist = []
                    # for ind in xrange(len(dtimes)):
                    #     dtime = dtimes[ind]
                    #     mydg = [ x for x in dg if oir.get_record(x)[dtind] == dtime ]
                    #     dglist.append( mydg )
                    # assert(len(dglist) == len(dtimes))
                    # for ind in xrange(len(dglist)):
                    # dg = list( set( dglist[ind] ) )
                    dg = list( set(dg) )
                    # dtime = dtimes[ind]
                    dtime = oir.get_record(dg[0])[dtind]
                    self.map_obj2group( groupnum = groupnum, groupset = dg )
                    self.map_group2dtime( groupnum = groupnum, dtime = dtime )
                    self.group2list[groupnum] = dg
                    groupnum += 1
                    if debugflag:
                        if count % 1000 == 99:
                            sys.stdout.write("#")
                            sys.stdout.flush()
                            sys.stdout.write(str(len(line)) + " | ")
        passnum = 1
        done = False
        while not done:
            print "Pass number: %d" % passnum
            done = self.clean_dtimes( objreader = oir )
            passnum += 1
        print "====[ PASS DONE ]==============================================================="
        self.merge_groups_with_same_dtime( objreader = oir, verify = True )
        #sys.stdout.write("\n")
        #sys.stdout.flush()
        print "----------------------------------------------------------------------"
        # TODO grlen = sorted( [ len(mylist) for group, mylist in self.group2list.iteritems() if len(mylist) > 0 ],
        #                       reverse = True )
        for gnum, mylist in self.group2list.iteritems():
            keylist = get_key_objects( mylist, oir )
            self.map_key2group( groupnum = groupnum, keylist = keylist )
            # Debug key objects. NOTE: This may not be used for now.
            if len(keylist) > 1:
                logger.error( "multiple key objects: %s" % str(keylist) )
                multkey += 1
            elif len(keylist) == 0:
                logger.critical( "NO key object in group: %s" % str(dg) )
                withoutkey += 1
        print "Multiple key: %d" % multkey
        print "Without key: %d" % withoutkey
        print "----------------------------------------------------------------------"

    def iteritems( self ):
        return self.group2list.iteritems()

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
                    sdict[row[0]] = int(row[1])
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

    def get_number_died_by_stack( self ):
        assert("died_by_stack" in self.summarydict)
        return self.summarydict["died_by_stack"]

    def get_number_died_by_heap( self ):
        assert("died_by_heap" in self.summarydict)
        return self.summarydict["died_by_heap"]

    def get_number_died_by_global( self ):
        assert("died_by_global" in self.summarydict)
        return self.summarydict["died_by_global"]

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
            "StabilityReader",
            "is_key_object", "get_index", "is_stable", ]

if __name__ == "__main__":
    main()
