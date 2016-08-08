from __future__ import division
# simplelist_analyze.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
# from operator import itemgetter
from collections import Counter
import csv
from datetime import datetime, date
import time
from collections import defaultdict

from mypytools import mean, stdev, variance
from garbology import SummaryReader, get_index

pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "simplelist_analyze.log",
                  logger_name = 'simplelist_analyze',
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

def debug_lifetimes( G, cycle, bmark, logger ):
    global pp
    for x in cycle:
        if G.node[x]["lifetime"] <= 0:
            n = G.node[x]
            # print "XXX %s: [ %d - %s ] lifetime: %d" % \
            #     (bmark, x, n["type"], n["lifetime"])
            logger.critical( "XXX: [ %d - %s ] lifetime: %d" %
                             (x, n["type"], n["lifetime"]) )

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

def get_summary( summary_path ):
    start = False
    done = False
    summary = []
    with open(summary_path) as fp:
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
                row[1] = int(row[1])
                summary.append(row)
    assert(done)
    return dict(summary)

g_regex = re.compile( "([^\$]+)\$(.*)" )
def is_inner_class( mytype ):
    global g_regex
    m = g_regex.match(mytype)
    return True if m else False

def render_histogram( histfile = None,
                      title = None ):
    outpng = histfile + ".png"
    cmd = [ "/data/rveroy/bin/Rscript",
            "/data/rveroy/pulsrc/etanalyzer/Rgraph/histogram.R", # TODO Hard coded for now.
            # Put into config. TODO TODO TODO
            histfile, outpng,
            "800", "800",
            title, ]
    print "Running histogram.R on %s -> %s" % (histfile, outpng)
    print "[ %s ]" % cmd
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    print "--------------------------------------------------------------------------------"
    for x in result:
        print x
    print "--------------------------------------------------------------------------------"

def write_histogram( results = None,
                     tgtbase  = None,
                     title = None ):
    # TODO Use a list and a for loop to refactor.
    tgtpath_totals = tgtbase + "-totals.csv"
    tgtpath_cycles = tgtbase + "-cycles.csv"
    tgtpath_types = tgtbase + "-types.csv"
    with open(tgtpath_totals, 'wb') as fp_totals, \
         open(tgtpath_cycles, 'wb') as fp_cycles, \
         open(tgtpath_types, 'wb') as fp_types:
        # TODO REFACTOR into a loop
        # TODO 2015-1103 - RLV TODO
        header = [ "benchmark", "total" ]
        csvw = {}
        csvw["totals"] = csv.writer( fp_totals,
                                     quotechar = '"',
                                     quoting = csv.QUOTE_NONNUMERIC )
        csvw["largest_cycle"] = csv.writer( fp_cycles,
                                            quotechar = '"',
                                            quoting = csv.QUOTE_NONNUMERIC )
        csvw["largest_cycle_types_set"] = csv.writer( fp_types,
                                                      quotechar = '"',
                                                      quoting = csv.QUOTE_NONNUMERIC )
        keys = csvw.keys()
        dframe = {}
        for key in keys:
            csvw[key].writerow( header )
            dframe[key] = []
        for benchmark, infodict in results.iteritems():
            for key in keys:
                assert( key in infodict )
                for item in infodict[key]:
                    row = [ benchmark, item ] if key == "totals" \
                          else [ benchmark, len(item) ]
                    dframe[key].append(row)
        sorted_result = [ (key, sorted( dframe[key], key = itemgetter(0) )) for key in keys ]
        for key, result in sorted_result:
            for csvrow in result:
                csvw[key].writerow( csvrow )
    # TODO TODO TODO TODO
    # TODO TODO TODO: SPAWN OFF THREAD
    # TODO TODO TODO TODO
    render_histogram( histfile = tgtpath_totals,
                      title = title )
    render_histogram( histfile = tgtpath_cycles,
                      title = title )
    render_histogram( histfile = tgtpath_types,
                      title = title )

def output_R( benchmark = None ):
    pass
    # Need benchmark.
    # TODO: Do we need this?

def create_work_directory( work_dir,
                           today = "",
                           timenow = "",
                           logger = None,
                           interactive = False ):
    os.chdir( work_dir )
    # Check today directory ---------------------------------------------------
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
    os.chdir( today )
    # Check timenow directory -------------------------------------------------
    if os.path.isfile(timenow):
        print "Can not create %s as directory." % timenow
        exit(11)
    if not os.path.isdir( timenow ):
        os.mkdir( timenow )
    else:
        print "WARNING: %s directory exists." % timenow
        logger.warning( "WARNING: %s directory exists." % timenow )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    os.chdir( timenow )
    return str(os.getcwd())

def print_summary( summary ):
    global pp
    for bmark, fdict in summary.iteritems():
        print "[%s]:" % bmark
        for key, value in fdict.iteritems():
            if key == "by_size":
                continue
            if key == "types" or key == "sbysize":
                print "    [%s]: %s" % (key, pp.pformat(value))
            else:
                print "    [%s]: %d" % (key, value)

def skip_benchmark(bmark):
    return ( bmark == "tradebeans" or # Permanent ignore
             bmark == "tradesoap" or # Permanent ignore
             bmark != "xalan"
             # bmark == "lusearch" or
             # ( bmark != "batik" and
             #   bmark != "lusearch" and
             #   bmark != "luindex" and
             #   bmark != "specjbb" and
             #   bmark != "avrora" and
             #   bmark != "tomcat" and
             #   bmark != "pmd" and
             #   bmark != "fop"
             # )
           )

def with_primitive_array( typeset = set([]) ):
    typelist = list(typeset)
    arre = re.compile("^\[[CIJ]")
    m1 = arre.search(typelist[1])
    m0 = arre.search(typelist[0])
    if ( (typelist[0].find("[L") == 0) and
         (m1 != None) ):
        return typelist[0]
    elif ( (typelist[1].find("[L") == 0) and
           (m0 != None) ):
        return typelist[1]
    return None

def fixed_known_key_objects( group = [],
                             objinfo = None,
                             logger = None ):
    # Get types
    typeset = set( [ objinfo.get_type(x) for x in group ] )
    logger.debug( "Checking group set: %s" % str(list(typeset)) )
    # Check against known groups
    if typeset == set( [ "[C", "Ljava/lang/String;" ] ):
        logger.debug( "Matches [C - String" )
        obj = None
        for x in group:
            if objinfo.get_type(x) == "Ljava/lang/String;":
                obj = x
                break
        assert(obj != None)
        return { "key" : "Ljava/lang/String;",
                 "obj" : obj }
    elif ( (len(typeset) == 2) and
           (with_primitive_array(typeset) != None) ):
        mytype = with_primitive_array(typeset)
        obj = None
        for x in group:
            if objinfo.get_type(x) == mytype:
                obj = x
                break
        assert(obj != None)
        return { "key" : "Ljava/lang/String;",
                 "obj" : obj }
    return None


def find_dupes( dgroups = None):
    count = 0 # Count for printing out debug progress marks
    # dash = 0 # Also for deub progress hash marks
    revdict = {}
    for groupnum, grouplist in dgroups.iteritems():
        #if count % 100 == 99:
        #    sys.stdout.write("-/")
        #    dash += 1
        #    if dash % 41 == 40:
        #        sys.stdout.write('\n')
        count += 1
        for mem in grouplist:
            if mem in revdict:
                revdict[mem].append( groupnum )
            else:
                revdict[mem] = [ groupnum ]
    # sys.stdout.write("\n")
    dupes = {}
    for objId, grlist in revdict.iteritems():
        if len(grlist) > 1:
            dupes[objId] = grlist
    return dupes

def get_last_edge_record( group, edgeinfo, objectinfo ):
    latest = 0 # Time of most recent
    srclist = []
    tgt = 0
    for obj in group:
        rec = edgeinfo.get_last_edge_record(obj)
        if rec == None:
            # No last edge
            # TODO Root object?
            assert( objectinfo.died_by_stack(obj) )
        elif rec["dtime"] > latest:
            latest = rec["dtime"]
            srclist = rec["lastsources"]
            tgt = obj
    return { "dtime" : latest,
             "lastsources" : srclist,
             "target" : tgt }

def is_array( mytype ):
    return (len(mytype) > 0) and (mytype[0] == "[")

def debug_primitive_key( group = None,
                         keytype = None,
                         keyId = None,
                         objinfo = None,
                         logger = None ):
    print "   -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-"
    print "    >>> PRIMITIVE KEY: %s" % keytype
    for x in group:
        tmp = objinfo.get_record(x)
        print "    %d [ %s ][ by %s ] - %d" % \
            (x, objinfo.get_type(x), tmp[ get_index("DIEDBY") ], tmp[ get_index("DTIME") ])
    print "   -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-"

def debug_multiple_keys( group = None,
                         key_objects = None,
                         objinfo = None,
                         logger = None ):
    print "   -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-"
    print "    >>> MULTIPLE KEY DEBUG:"
    print "        [KEYS]"
    for x in key_objects:
        tmp = objinfo.get_record(x)
        print "    %d [ %s ][ by %s ] - %d" % \
            (x, objinfo.get_type(x), tmp[ get_index("DIEDBY") ], tmp[ get_index("DTIME") ])
    print "   Others:", str( list(set([ objinfo.get_type(x) for x in group if x not in key_objects ])) )
    print "   -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-"

def is_primitive_type( mytype = None ):
    return ( (mytype == "Z")     # boolean
              or (mytype == "B") # byte
              or (mytype == "C") # char
              or (mytype == "D") # double
              or (mytype == "F") # float
              or (mytype == "I") # int
              or (mytype == "J") # long
              or (mytype == "S") # short
              )

def is_primitive_array( mytype = None ):
    # Is it an array?
    if not is_array(mytype):
        return False
    else:
        return ( is_primitive_type(mytype[1:]) or
                 is_primitive_array(mytype[1:]) )
        
# Return true if all objects in group are:
#     - primitive
#     - primitive arrays
def all_primitive_types( group = [],
                         objinfo = None ):
    for obj in group:
        mytype = objinfo.get_type(obj)
        if not is_primitive_type(mytype) and not is_primitive_array(mytype):
            return False
    return True


def get_earliest_alloctime_object( group = [],
                                   objinfo = None ):
    if len(group) == 0:
        return None
    cur = group[0]
    currec = objinfo.get_record(cur)
    cur_atime = currec[ get_index("ATIME") ]
    # dtime = rec[ get_index("DTIME") ]
    for obj in group:
        tmp = group[0]
        tmprec = objinfo.get_record(tmp)
        tmp_atime = tmprec[ get_index("ATIME") ]
        if tmp_atime < cur_atime:
            cur = tmp
            currec = tmprec
            cur_atime = tmp_atime
    return cur

def get_most_likely_keytype( objlist = [],
                             tydict = {} ):
    blacklist = set([ "Ljava/lang/String;", ])
    assert(len(objlist)) > 0
    newlist = list( set( [ x for x in objlist
                           if (tydict[x] not in blacklist or is_primitive_type(tydict[x])) ] ) )
    if len(newlist) > 1:
        # Let's return the oldest object
        newlist = sorted(newlist)
    if len(newlist) > 0:
        return newlist[0]
    # Something in the blacklist is the key object
    newlist = list( set( [ x for x in objlist
                           if is_primitive_type(tydict[x]) ] ) )
    if len(newlist) > 1:
        # Let's return the oldest object
        newlist = sorted(newlist)
    if len(newlist) > 0:
        return newlist[0]
    # What does this mean?
    print "--------------------------------------------------------------------------------"
    print "DEBUG: blacklist doesn't work for -->"
    print str(tydict)
    print "--------------------------------------------------------------------------------"
    return objlist[0]

def update_keytype_dict( ktdict = {},
                         objId = -1,
                         objType = "",
                         group = [],
                         objinfo = None,
                         contextinfo = None,
                         group_types = frozenset([]),
                         max_age = 0,
                         dumpall = False,
                         filterbytes = 8388608,
                         writer = None,
                         logger = None ):

    assert( objId >= 0 )
    if dumpall:
        assert( writer != None )
    if objinfo.died_at_end(objId):
        # We ignore immortal objects that died at the END.
        return DIEDBYSTACK
    grouplen = len(group)
    early_obj = get_earliest_alloctime_object( group = group, objinfo = objinfo )
    true_key_flag = (early_obj == objId)
    if objType in ktdict:
        ktdict[objType]["max"] = max( grouplen, ktdict[objType]["max"] )
        ktdict[objType]["min"] = min( grouplen, ktdict[objType]["min"] )
        ktdict[objType]["grouplen_list"].append( grouplen )
        ktdict[objType]["total"] += 1
        ktdict[objType]["group_types"].update( [ group_types ] )
        ktdict[objType]["allocsites"].update( [ objinfo.get_allocsite(objId) ] )
        if true_key_flag:
            ktdict[objType]["true_key_count"] += 1
    else:
        ktdict[objType] = { "total" : 1,
                            "max" : grouplen,
                            "min" : grouplen,
                            "grouplen_list" : [ grouplen ],
                            "is_array": is_array(objType),
                            "group_types" : Counter( [ group_types ] ),
                            "true_key_count" : 1 if true_key_flag else 0,
                            "allocsites" : Counter( [ objinfo.get_allocsite(objId) ] ), }
    # Also update the context information
    cpair = objinfo.get_death_context( objId )
    if dumpall:
        # Header is [ "type", "time", "context1", "context2",
        #             "number of objects", "cause", "subcause",
        #             "allocsite", "age_methup", "age_alloc" ]
        if ( (filterbytes > 0) and (max_age <= filterbytes) ):
            rec = objinfo.get_record(objId)
            dcause = objinfo.get_death_cause_using_record(rec)
            subcause = ( objinfo.get_stack_died_by_attr_using_record(rec) if dcause == "S"
                         else ( objinfo.get_last_heap_update_using_record(rec)
                                if (dcause == "H" or dcause == "G") else "NONE" ) )
            age_methup = objinfo.get_age_using_record(rec)
            age_alloc = objinfo.get_age_using_record_ALLOC(rec)
            writer.writerow( [ objType,
                               objinfo.get_death_time_using_record(rec),
                               cpair[0], # death context pair first element
                               cpair[1], # death context pair second element
                               # TODO: which direction does the context pair go?
                               grouplen,
                               dcause,
                               subcause,
                               # TODO: Use a context pair for allocation site too?
                               objinfo.get_allocsite_using_record(rec),
                               age_methup,
                               age_alloc, ] )
        else:
            logger.debug( "Object [%s](%d) IGNORED." % (objType, objId) )
    result = contextinfo.inc_key_count( context_pair = cpair,
                                        objType = objType )
    if result == None:
        return False
    elif not result:
        sys.stdout.write( "ERR: objId[%d] -> %s" % (objId, str(cpair)) )
        return False
    return True

ONEKEY = 1
MULTKEY = 2
NOKEY = 3
ONEKEY_LASTEDGE = 4
ONEKEY_KNOWNOBJ = 5
DIEDBYSTACK = 7
DIEDATEND = 8
NOTFOUND = 9
def get_key_object_types( gnum = None,
                          ktdict = {},
                          dgroups = None,
                          edgeinfo = None,
                          objinfo = None,
                          contextinfo = None,
                          contextresult = {},
                          dumpall = False,
                          filterbytes = 8388608,
                          logger = None,
                          writer = None,
                          dgraph = None,
                          ignore_died_at_end = True ):
    if gnum in dgroups.group2list:
        group = dgroups.group2list[gnum] 
    else:
        return NOTFOUND # TODO What should return be? None?
    # Check if any of the group is a key object
    key_objects = []
    max_age = 0 
    for xtmp in group:
        if objinfo.is_key_object(xtmp):
            key_objects.append(xtmp)
        max_age = max( max_age, objinfo.get_age_ALLOC(xtmp) )
    found_key = False
    used_last_edge = False
    print " - grouplen: %d" % len(group)
    # Check to see if the key object is a primitive type array
    total_cc = 0
    err_cc = 0
    # ======================================================================
    if all_primitive_types( group, objinfo ):
        print " - all primitive types."
        # All are a group unto themselves
        for obj in group:
            # NOTE: Ignoring objects that died at end (ie IMMORTAL)
            # TODO: We may not want this behavior for the graph...
            # TODO: ...or even for everything? -RLV
            if objinfo.died_at_end(obj):
                continue # TODO Is this what we want in the analysis?
            tmptype = objinfo.get_type(obj)
            result = update_keytype_dict( ktdict = ktdict,
                                          objId = obj,
                                          objType = tmptype,
                                          group = [ obj ],
                                          objinfo = objinfo,
                                          contextinfo = contextinfo,
                                          group_types = frozenset([]),
                                          max_age = max_age,
                                          filterbytes = filterbytes,
                                          dumpall = dumpall,
                                          writer = writer,
                                          logger = logger )
            keyrec = objinfo.get_record(obj)
            atime = objinfo.get_alloc_time_using_record( keyrec )
            dtime = objinfo.get_death_time_using_record( keyrec )
            dgraph.add_node( gnum, { "size" : 1,
                                     "keytype" : str(tmptype),
                                     "atime" : atime,
                                     "ditme" : dtime, } )
            # Get dtime and atime using objId
            total_cc += 1
            err_cc = ((err_cc + 1) if (not result) else err_cc)
        # print "BY STACK - all primitive" # TODO Make into a logging statement
        contextresult["total"] = contextresult["total"] + total_cc
        contextresult["error"] = contextresult["error"] + err_cc
        return DIEDBYSTACK # TODO This does not seem right.
    # ======================================================================
    if len(key_objects) == 1:
        # Found key objects
        found_key = True
        tgt = key_objects[0]
        result = ONEKEY
        print " - single key object: %s" % objinfo.get_type(tgt)
        if objinfo.died_at_end(tgt):
            return DIEDATEND
        mytype = objinfo.get_type(tgt)
        if mytype == "[C":
            tylist = [ objinfo.get_type(x) for x in group ]
            if ( ("Ljava/lang/String;" in tylist) and
                 (len(group) == 2) ):
                mytype = "Ljava/lang/String;"
            else:
                print "Y:", str(tylist)
                return NOTFOUND
    else:
        # ======================================================================
        if len(key_objects) > 1:
            # Multiple keys?
            tgt = None
            debug_multiple_keys( group = group,
                                 key_objects = key_objects,
                                 objinfo = objinfo,
                                 logger = logger )
            if objinfo.died_at_end(group[0]):
                return DIEDATEND
            result = MULTKEY
            print " - Multiple key objects."
        else:
            print " - DEBUG: NO marked key objects."
        done = False
        curindex = 0
        while not done and (curindex < len(group)):
            cur = group[curindex]
            currec = objinfo.get_record(cur)
            cur_dtime = currec[ get_index("DTIME") ]
            curtype = objinfo.get_type(cur)
            if is_primitive_array(curtype) or is_primitive_type(curtype):
                curindex += 1
                continue
            else:
                done = True
                break
        if not done or curindex >= len(group):
            return NOTFOUND
        curset = set([ cur ])
        curtydict = { cur : curtype }
        for tmp in group[curindex:]:
            tmprec = objinfo.get_record(tmp)
            tmp_dtime = tmprec[ get_index("DTIME") ]
            tmptype = objinfo.get_type(tmp)
            if is_primitive_array(tmptype) or is_primitive_type(tmptype):
                continue
            elif tmp_dtime > cur_dtime:
                curset = set([ tmp ])
                currec = tmprec
                cur_dtime = tmp_dtime
                curtydict = { tmp : tmptype }
            elif tmp_dtime == cur_dtime:
                if tmp not in curset:
                    curset.add( tmp )
                    curtydict[tmp] = tmptype
        if len(curset) > 1:
            print "--------------------------------------------------------------------------------"
            print curset
            print "--------------------------------------------------------------------------------"
            for obj, mytype in curtydict.iteritems():
                print "%d -> %s" % (obj, mytype)
            likely = get_most_likely_keytype( objlist = list(curset),
                                              tydict = curtydict )
            curset = set([ likely ])
        assert(len(curset) > 0 )
        tgt = list(curset)[0]
        mytype = curtydict[tgt]
        # TODO Make into a logging statement
        print "  - key among multiples - %d [ %s ][ dtime: %d ]" % (cur, curtype, cur_dtime)
        if mytype == "[C":
            tylist = [ objinfo.get_type(x) for x in group ]
            if ( ("Ljava/lang/String;" in tylist) and
                 (len(group) == 2) ):
                mytype = "Ljava/lang/String;"
            else:
                print "Y:", str(tylist)
                return NOTFOUND
    # ----------------------------------------------------------------------------------
    group_types = frozenset( [ objinfo.get_type(x) for x in group if x != tgt ] )
    is_array_flag = is_array(mytype)
    is_primitive_key = is_primitive_array(mytype)
    if is_primitive_key and len(group_types) > 0:
        debug_primitive_key( group = [ x for x in group if x != tgt ],
                             keytype = mytype,
                             keyId = tgt,
                             objinfo = objinfo,
                             logger = logger )
    result = update_keytype_dict( ktdict = ktdict,
                                  objId = tgt,
                                  objType = mytype,
                                  group = group,
                                  objinfo = objinfo,
                                  contextinfo = contextinfo,
                                  group_types = group_types,
                                  filterbytes = filterbytes,
                                  max_age = max_age,
                                  dumpall = dumpall,
                                  writer = writer,
                                  logger = logger )
    # Add to graph
    keyrec = objinfo.get_record(tgt)
    atime = objinfo.get_alloc_time_using_record( keyrec )
    dtime = objinfo.get_death_time_using_record( keyrec )
    dgraph.add_node( gnum, { "size" : len(group),
                             "keytype" : str(mytype),
                             "atime" : atime,
                             "ditme" : dtime, } )
    total_cc += 1
    err_cc = ((err_cc + 1) if not result else err_cc)
    # This looks like all debug.
    # TODO print "-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
    # TODO print "%s:" % mytype
    # TODO if is_array(mytype) and len(group_types) > 0:
    # TODO     print " --- DEBUG:"
    # TODO     print "%d [ %s ] - %d" % (tgt, objinfo.get_type(tgt), objinfo.get_death_time(tgt))
    # TODO     for x in group:
    # TODO         tmptype = objinfo.get_type(x)
    # TODO         if x != tgt:
    # TODO             tmp = objinfo.get_record(x)
    # TODO             print "%d [ %s ][ by %s ] - %d" % \
    # TODO                (x, objinfo.get_type(x), tmp[ get_index("DIEDBY") ], tmp[ get_index("DTIME") ])
    # TODO else:
    # TODO     for t in group_types:
    # TODO         print t,
    # TODO print
    # TODO print "-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
    contextresult["total"] = contextresult["total"] + total_cc
    contextresult["error"] = contextresult["error"] + err_cc
    return ONEKEY

def check_host( benchmark = None,
                worklist_config = {},
                host_config = {} ):
    thishost = socket.gethostname()
    for wanthost in worklist_config[benchmark]:
        if thishost in host_config[wanthost]:
            return True
    return False

def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None

def main_process( output = None,
                  global_config = {},
                  summary_config = {},
                  main_config = {},
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO 2016 August 7 TODO
    # This is where the summary CSV files are
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Setup stdout to file redirect TODO: Where should this comment be placed?
    # TODO: Eventually remove the following commented code related to hosts.
    # Since we're not doing mutiprocessing, we don't need this. But keep
    # it here until absolutely sure.
    # TODO: thishost = get_actual_hostname( hostname = socket.gethostname().lower(),
    # TODO:                                 host_config = host_config )
    # TODO: assert( thishost != None )
    # TODO: thishost = thishost.upper()
    # Get the date and time to label the work directory.
    today = date.today()
    today = today.strftime("%Y-%m%d")
    timenow = datetime.now().time().strftime("%H-%M-%S")
    olddir = os.getcwd()
    os.chdir( main_config["output"] )
    workdir = create_work_directory( work_dir = main_config["output"],
                                     today = today,
                                     timenow = timenow,
                                     logger = logger,
                                     interactive = False )
    # Take benchmarks to process from etanalyze_config
    # The benchmarks are:
    #     BENCHMARK   |   CREATE  |  DELETE   |
    #     simplelist1 |    seq    |    seq    |
    #     simplelist2 |   rand    |    seq    |
    #     simplelist3 |    seq    |    at end |
    #     simplelist4 |   rand    |    at end |
    # Where to get file?
    # Filename is in "summary_config"
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    assert( "simplelist1" in summary_config )
    assert( "simplelist2" in summary_config )
    assert( "simplelist3" in summary_config )
    assert( "simplelist4" in summary_config )
    # Give simplelist? more descriptive names
    slist = { "SEQ-SEQ" : {}, # simplelist1
              "RAND-SEQ" : {}, # simplelist2
              "SEQ-ATEND" : {}, # simplelist3
              "RAND-ATEND" : {}, } # simplelist4
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    print "XXX:", os.path.join( cycle_cpp_dir, summary_config["simplelist1"] )
    slist["SEQ-SEQ"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                               summary_config["simplelist1"] ) )
    slist["RAND-SEQ"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                summary_config["simplelist2"] ) )
    slist["SEQ-ATEND"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                 summary_config["simplelist3"] ) )
    slist["RAND-ATEND"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                  summary_config["simplelist4"] ) )

    print "====[ Reading in the summaries ]================================================"
    for skind, mydict in slist.iteritems():
        sreader = mydict["sreader"]
        sreader.read_summary_file()
        pp.pprint( sreader.__get_summarydict__() )
    print "DONE reading all 4."
    print "================================================================================"
    print "simplelist_analyze.py - DONE."
    os.chdir( olddir )
    exit(0)

def config_section_map( section, config_parser ):
    result = {}
    options = config_parser.options(section)
    for option in options:
        try:
            result[option] = config_parser.get(section, option)
        except:
            print("exception on %s!" % option)
            result[option] = None
    return result

def process_config( args ):
    assert( args.config != None )
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( args.config )
    global_config = config_section_map( "global", config_parser )
    summary_config = config_section_map( "summary_cpp", config_parser )
    main_config = config_section_map( "simplelist-analyze", config_parser )
    # MAYBE: objectinfo_config = config_section_map( "objectinfo", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    # PROBABLY NOT: worklist_config = config_section_map( "dgroups-worklist", config_parser )
    return { "global" : global_config,
             "summary" : summary_config,
             "main" : main_config,
             # "objectinfo" : objectinfo_config,
             # "contextcount" : contextcount_config,
             # "host" : host_config,
             # "worklist" : worklist_config
             }

def process_host_config( host_config = {} ):
    for bmark in list(host_config.keys()):
        hostlist = host_config[bmark].split(",")
        host_config[bmark] = hostlist
    return defaultdict( list, host_config )

def process_worklist_config( worklist_config = {} ):
    mydict = defaultdict( lambda: "NONE" )
    for bmark in list(worklist_config.keys()):
        hostlist = worklist_config[bmark].split(",")
        mydict[bmark] = hostlist
    return mydict

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "output", help = "Target output filename." )
    parser.add_argument( "--config",
                         help = "Specify configuration filename.",
                         action = "store" )
    parser.add_argument( "--debug",
                         dest = "debugflag",
                         help = "Enable debug output.",
                         action = "store_true" )
    parser.add_argument( "--no-debug",
                         dest = "debugflag",
                         help = "Disable debug output.",
                         action = "store_false" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "simplelist_analyze.log",
                         debugflag = False,
                         config = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    assert( args.config != None )
    # Get the configurations we need.
    configdict = process_config( args )
    global_config = configdict["global"]
    summary_config = configdict["summary"]
    main_config = configdict["main"]
    # PROBABLY DELETE:
    # contextcount_config = configdict["contextcount"]
    # objectinfo_config = configdict["objectinfo"]
    # host_config = process_host_config( configdict["host"] )
    # worklist_config = process_worklist_config( configdict["worklist"] )
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         global_config = global_config,
                         summary_config = summary_config,
                         main_config = main_config,
                         # contextcount_config = contextcount_config,
                         # objectinfo_config = objectinfo_config,
                         # host_config = host_config,
                         # worklist_config = worklist_config,
                         logger = logger )

if __name__ == "__main__":
    main()
