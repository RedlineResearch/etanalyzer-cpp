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
        self.logger = logger

    def is_key_object( self, objId = None ):
        assert(type(objId) == type(1))
        if objId in self.objdict:
            od = self.objdict
            return ( od[objId][get_index("GARBTYPE")] == "CYCKEY" or
                     od[objId][get_index("GARBTYPE")] == "DAGKEY" )
        else:
            return False

    def read_objinfo_file( self ):
        start = False
        done = False
        object_info = self.objdict
        with get_trace_fp( self.objinfo_file_name, self.logger ) as fp:
            for line in fp:
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
                    if objId not in object_info:
                        object_info[objId] = tuple(row)
                        if self.is_key_object( objId ):
                            self.keyset.add( objId )
                    else:
                        self.logger.error( "DUPE: %s" % str(objId) )
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
        return self.objdict.iteritems()

    def iterrecs( self ):
        odict = self.objdict
        keys = odict.keys()
        for objId in keys:
            yield (objId, odict[objId])

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
        edge_info = self.edgedict
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
                    row = [ int(x) for x in rowtmp ]
                    src = row[0]
                    tgt = row[1]
                    timepair = tuple(row[2:])
                    dtime = row[3]
                    self.edgedict[tuple([src, tgt])] = timepair
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
        for edge, timepaid in self.edgedict.iteritems():
            print "(%d, %d) -> (%d, %d)" % (edge[0], edge[1], timepaid[0], timepaid[1])
            count += 1
            if numlines != 0 and count >= numlines:
                break

    def get_edge_times( self, edge = None ):
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
        return self.obj2group[groupnum] if groupnum in self.obj2group else []

    def get_group_number( self, objId = 0 ):
        def _group_len(objId):
            return len(self.group2list[objId])
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
                    # dtimes = list( set( [ oir.get_record(x)[dtind] for x in dg ] ) )
                    # if (len(dtimes) > 1):
                    #     # split into groups according to death times
                    #     logger.debug( "Multiple death times: %s" % str(dtimes) )
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
        #sys.stdout.write("\n")
        #sys.stdout.flush()
        #print "DUPES:", len(dupeset)
        #print "TOTAL:", len(seenset)
        # TODO moved = {}
        # TODO loopflag = True
        # TODO while loopflag:
        # TODO     loopflag = False
        # TODO     for obj, groups in self.obj2group.iteritems():
        # TODO         if len(groups) > 1:
        # TODO             # an object is in multiple groups
        # TODO             # Merge into lower group number.
        # TODO             gsort = sorted( [ x for x in groups if (x not in moved and x in self.group2list) ] )
        # TODO             if len(gsort) < 2:
        # TODO                 logger.debug( "Continuing on length < 2 for objId[ %d ]." % obj )
        # TODO                 continue
        # TODO             stackflag =  True
        # TODO             for gtmp in gsort:
        # TODO                 stackflag = stackflag and oir.verify_died_by( grouplist = self.group2list[gtmp],
        # TODO                                                               died_by = "S" )
        # TODO             if stackflag:
        # TODO                 logger.debug( "Continuing on BY STACK for objId[ %d ]." % obj )
        # TODO                 continue
        # TODO             tgt = gsort[0]
        # TODO             logger.debug( "Merging into group %d for objId[ %d ]." % (tgt, obj) )
        # TODO             for gtmp in gsort[1:]:
        # TODO                 # Add to target group
        # TODO                 if gtmp in self.group2list:
        # TODO                     loopflag = True
        # TODO                     self.group2list[tgt].extend( self.group2list[gtmp] )
        # TODO                     moved[gtmp] = tgt
        # TODO                     # Remove the merged group
        # TODO                     del self.group2list[gtmp]
        # TODO                     # TODO TODO TODO
        # TODO                     # Fix the obj2group when we delete from group2list
        # TODO                 # TODO Should we remove from other dictionaries?
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
                    rowtmp = line.split(",")
                    # 0 - key
                    # 1 - value
                    row = [ int(x) for x in rowtmp ]
                    sdict[row[0]] = row[1]
        assert(done)

    def edgedict_iteritems( self ):
        return self.edgedict.iteritems()

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

    def print_out( self ):
        for key, val in self.summarydict.iteritems():
            print "%s -> %d" % (key, val)


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
                  debugflag = False,
                  verbose = False ):
    global pp
    gconfig.print_all_config( pp )
    gconfig.verify_all_exist( printflag = verbose )
    print "===========[ DONE ]==================================================="

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "config",
                         help = "Specify configuration filename." )
    parser.add_argument( "--verbose",
                         dest = "verbose",
                         help = "Enable verbose output.",
                         action = "store_true" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "garbology.log",
                         verbose = False,
                         config = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    assert( args.config != None )
    assert( os.path.isfile( args.config ) )
    gconfig = GarbologyConfig( args.config )
    debugflag = gconfig.global_cfg["debug"]
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = debugflag )
    #
    # Main processing
    #
    return main_process( logger = logger,
                         gconfig = gconfig,
                         debugflag = debugflag,
                         verbose = args.verbose )

__all__ = [ "EdgeInfoReader", "GarbologyConfig", "ObjectInfoReader",
            "ContextCountReader", "is_key_object", "get_index", ]

if __name__ == "__main__":
    main()
