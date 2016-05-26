# dgroup_analyze-MPR.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
from operator import itemgetter
from collections import Counter
import csv
import subprocess
from datetime import datetime, date
import time
import socket
from collections import defaultdict
from multiprocessing import Process

# TODO from itertools import combinations

from mypytools import mean, stdev, variance
from garbology import EdgeInfoReader, ObjectInfoReader, DeathGroupsReader, get_index

pp = pprint.PrettyPrinter( indent = 4 )

ATIME = 0
DTIME = 1
SIZE = 2
TYPE = 3
REASON = 4

def setup_logger( targetdir = ".",
                  filename = "dgroup_analyze-MPR.log",
                  logger_name = 'dgroup_analyze-MPR',
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

def create_edge_dictionary( edges = None ):
    edgedict = {}
    for edge in edges:
        src = edge[0]
        tgt = edge[1]
        if src not in edgedict:
            edgedict[src] = [ tgt ]
        else:
            edgedict[src].append(tgt)
    for src, tgtlist in edgedict.iteritems():
        edgedict[src] = set(tgtlist)
    return edgedict

def get_types( G, cycle ):
    return [ G.node[x]["type"] for x in cycle ]

def get_types_and_save_index( G, cycle ):
    return [ (x, G.node[x]["type"]) for x in cycle ]

def get_lifetimes( G, cycle ):
    return [ G.node[x]["lifetime"] for x in cycle ]

def get_lifetimes_debug( G, cycle ):
    result = []
    for x in cycle:
        try:
            mynode = G.node[x]
        except:
            print "Unable to get node[ %d ]" % x
            continue
        try:
            mylifetime = mynode["lifetime"]
        except:
            print "Unable to get lifetime for node[ %d ] -> %s" % (x, str(mynode))
            continue
        result.append(mylifetime)
    return result

def get_sizes( G, cycle ):
    return [ G.node[x]["size"] for x in cycle ]

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

def get_edge_info( edgeinfo_path ):
    start = False
    done = False
    edge_info = {}
    with open(edgeinfo_path) as fp:
        # Map edge (src,tgt) -> (alloctime, deathtime)
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
                row = tuple([ int(x) for x in rowtmp[2:] ])
                edge_info[ (int(rowtmp[0]), int(rowtmp[1])) ] = row
    assert(done)
    return edge_info

def get_cycles( tgtpath ):
    global pp
    with open(tgtpath) as fp:
        start = False
        done = False
        cycles = []
        for line in fp:
            line = line.rstrip()
            if line.find("---------------[ CYCLES") == 0:
                start = not start
                if start:
                    continue
                else:
                    done = True
                    break
            if start:
                line = line.rstrip(",")
                row = line.split(",")
                row = [ int(x) for x in row ]
                cycles.append(row)
    assert(done) 
    return cycles


def get_cycle_info_list( cycle = None,
                         objinfo_dict = None,
                         # objdb = None,
                         logger = None ):
    cycle_info_list = []
    odict = objinfo_dict
    for node in cycle:
        try:
            # rec = objdb.get_record(node)
            rec = odict[node]
            mytype = rec[TYPE]
            mysize = rec[SIZE]
            atime = rec[ATIME]
            dtime = rec[DTIME]
            lifetime = (dtime - atime) if ((dtime > atime) and  (dtime != 0)) \
                else 0
            cycle_info_list.append( (node, mytype, mysize, lifetime) )
        except:
            logger.critical("Missing node[ %s ]" % str(node))
            mytype = "<NONE>"
            mysize = 0
            lifetime = 0
            cycle_info_list.append( (node, mytype, mysize, lifetime) )
    return cycle_info_list

g_regex = re.compile( "([^\$]+)\$(.*)" )
def is_inner_class( mytype ):
    global g_regex
    m = g_regex.match(mytype)
    return True if m else False

def row_to_string( row ):
    result = None
    strout = StringIO.StringIO()
    csvwriter = csv.writer(strout)
    # Is the list comprehension necessary? Doesn't seem like it.
    csvwriter.writerow( [ x for x in row ] )
    result = strout.getvalue()
    strout.close()
    return result.replace("\r", "")

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
                           thishost = "",
                           today = "",
                           timenow = "",
                           logger = None, interactive = False ):
    os.chdir( work_dir )
    # Check to see host name directory ----------------------------------------
    if os.path.isfile(thishost):
        print "%s is a file, NOT a directory." % thishost
        exit(11)
    if not os.path.isdir( thishost ):
        os.mkdir( thishost )
        print "WARNING: %s directory does not exist. Creating it" % thishost
        logger.warning( "WARNING: %s directory exists." % thishost )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    os.chdir( thishost )
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

def save_interesting_small_cycles( largest_scc, summary ):
    # Interesting is defined to be 4 or smaller
    length = len(largest_scc)
    if length > 0 and length <= 4:
        summary["by_size"][length].append( largest_scc )

def get_last_edge_from_result( edge_list ):
    ledge = edge_list[0]
    latest = ledge[4]
    for newedge in edge_list[1:]:
        if newedge[4] > latest:
            ledge = newedge
    return ledge

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
                          logger = None,
                          ignore_died_at_end = True ):
    if gnum in dgroups.group2list:
        group = dgroups.group2list[gnum] 
    else:
        return NOTFOUND # TODO What should return be? None?
    # Check if any of the group is a key object
    key_objects = [ x for x in group if objinfo.is_key_object(x) ]
    found_key = False
    used_last_edge = False
    print " - grouplen: %d" % len(group)
    # Check to see if the key object is a primitive type array
    if all_primitive_types( group, objinfo ):
        print " - all primitive types."
        # All are a group unto themselves
        for obj in group:
            if objinfo.died_at_end(obj):
                continue
            tmptype = objinfo.get_type(obj)
            if tmptype in ktdict:
                ktdict[tmptype]["max"] = max( 1, ktdict[tmptype]["max"] )
                ktdict[tmptype]["total"] += 1
            else:
                ktdict[tmptype] = { "total" : 1,
                                    "max" : 1,
                                    "is_array" : is_array(tmptype),
                                    "group_types" : Counter( [ frozenset([]) ] ) }
        # print "BY STACK - all primitive" # TODO Make into a logging statement
        return DIEDBYSTACK # TODO This does not seem right.
    if len(key_objects) == 1:
        # Found key objects
        found_key = True
        tgt = key_objects[0]
        result = ONEKEY
        print " - single key object: %s" % objinfo.get_type(tgt)
        if objinfo.died_at_end(tgt):
            return DIEDATEND
        mytype = objinfo.get_type(tgt)
    else:
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
            if is_primitive_array(curtype) or is_primitive(curtype):
                curindex += 1
                continue
            else:
                done = True
                break
        if not done or curindex >= len(group):
            return NOTFOUND
        for tmp in group[curindex:]:
            tmprec = objinfo.get_record(tmp)
            tmp_dtime = currec[ get_index("DTIME") ]
            tmptype = objinfo.get_type(tmp)
            if is_primitive_array(tmptype) or is_primitive(tmptype):
                continue
            elif tmp_dtime > cur_dtime:
                cur = tmp
                currec = tmprec
                cur_dtime = tmp_dtime
                curtype = tmptype
        tgt = cur
        mytype = curtype
        # TODO Make into a logging statement
        print "  - key among multiples - %d [ %s ][ dtime: %d ]" % (cur, curtype, cur_dtime)
        # # First try the known groups
        # key_objects = fixed_known_key_objects( group = group,
        #                                        objinfo = objinfo,
        #                                        logger = logger )
        # if key_objects != None:
        #     tgt = key_objects["obj"]
        #     result = ONEKEY_KNOWNOBJ
        #     print " - found known key object [%s]" % objinfo.get_type(tgt)
        #     if objinfo.died_at_end(tgt):
        #         return DIEDATEND
        # else:
        #     pass
    group_types = frozenset( [ objinfo.get_type(x) for x in group if x != tgt ] )
    is_array_flag = is_array(mytype)
    is_primitive_key = is_primitive_array(mytype)
    if is_primitive_key and len(group_types) > 0:
        debug_primitive_key( group = [ x for x in group if x != tgt ],
                             keytype = mytype,
                             keyId = tgt,
                             objinfo = objinfo,
                             logger = logger )
    if mytype in ktdict:
        if objinfo.died_at_end(tgt):
            return DIEDBYSTACK
        ktdict[mytype]["max"] = max( 1, ktdict[mytype]["max"] )
        ktdict[mytype]["total"] += 1
        ktdict[mytype]["group_types"].update( [ group_types ] )
    else:
        ktdict[mytype] = { "total" : 1,
                           "max" : len(group),
                           "is_array": is_array(mytype),
                           "group_types" : Counter( [ group_types ] ) }
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

def __TODO_DELTE_LAST_EDGE():
    lastrec = get_last_edge_record( group, edgeinfo, objinfo )
    # print "%d @ %d : %d -> %s" % ( gnum,
    #                                lastrec["dtime"],
    #                                lastrec["target"],
    #                                str(lastrec["lastsources"]) )
    if len(lastrec["lastsources"]) == 1:
        # Get the type
        used_last_edge = True
        tgt = lastrec["target"]
        print " - last edge successful [%s]" % objinfo.get_type(tgt)
        if objinfo.died_at_end(tgt):
            return DIEDATEND
    elif len(lastrec["lastsources"]) > 1:
        print " - last edge has too many candidates. NO KEY OBJECT FOUND."
        return NOTFOUND
        # No need to do anything becuase this isn't a key object?
        # But DO we need to update the counts of the death groups above TODO
    elif len(lastrec["lastsources"]) == 0:
        # Means stack object?
        stackflag = objinfo.group_died_by_stack(group)
        if not stackflag:
            print "   -X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-"
            print "   No last edge but group didn't die by stack as a whole:"
            for obj in group:
                rec = objinfo.get_record( obj )
                print "       [%d] : %s -> %s (%s)" % ( obj,
                                                        rec[ get_index("TYPE") ],
                                                        rec[ get_index("DIEDBY") ],
                                                        "DAE" if objinfo.died_at_end(obj) else "---" )
            print "   -X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-"
        else:
            print "   died by stack. Making each object its own key object."
            # Died by stack. Each object is its own key object
            for obj in group:
                if objinfo.died_at_end(obj):
                    print " - ignoring DIED AT END."
                    continue
                tmptype = objinfo.get_type(obj)
                if tmptype in ktdict:
                    ktdict[tmptype]["max"] = max( len(group), ktdict[tmptype]["max"] )
                    ktdict[tmptype]["total"] += 1
                    ktdict[mytype]["group_types"].update( [ frozenset([])] )
                else:
                    is_array_flag = is_array(tmptype)
                    ktdict[tmptype] = { "total" : 1,
                                        "max" : len(group),
                                        "is_array": is_array_flag,
                                        "group_types" : Counter( [ frozenset([]) ] ) }
                return DIEDBYSTACK

def death_group_analyze( bmark = None,
                         cycle_cpp_dir = "",
                         main_config = {},
                         dgroups_filename = "",
                         objectinfo_config = {},
                         edge_config = {},
                         host_config = {},
                         logger = None ):
    logger.debug( "[%s]:================================================================"
                  % bmark )
    # TODO TODO 
    workdir = os.getcwd()
    outputfile = os.path.join( workdir,
                               "%s-OUTPUT.txt" % bmark )
    sys.stdout = open( outputfile, "wb" )
    # Get object dictionary information that has types and sizes
    # TODO
    # typedict = {}
    # rev_typedict = {}
    objectinfo_path = os.path.join(cycle_cpp_dir, objectinfo_config[bmark])
    objinfo = ObjectInfoReader( objectinfo_path, logger = logger )
    # ----------------------------------------
    logger.debug( "[%s]: Reading OBJECTINFO file..." % bmark )
    sys.stdout.write(  "[%s]: Reading OBJECTINFO file...\n" % bmark )
    oread_start = time.clock()
    objinfo.read_objinfo_file()
    oread_end = time.clock()
    logger.debug( "[%s]: DONE: %f" % (bmark, (oread_end - oread_start)) )
    sys.stdout.write(  "[%s]: DONE: %f\n" % (bmark, (oread_end - oread_start)) )
    # ----------------------------------------
    logger.debug( "[%s]: Reading EDGEINFO file..." % bmark )
    sys.stdout.write(  "[%s]: Reading EDGEINFO file...\n" % bmark )
    edgeinfo_path = os.path.join( cycle_cpp_dir, edge_config[bmark] )
    edgeinfo = EdgeInfoReader( edgeinfo_path, logger = logger )
    eread_start = time.clock()
    edgeinfo.read_edgeinfo_file()
    eread_end = time.clock()
    logger.debug( "[%s]: DONE: %f" % (bmark, (eread_end - eread_start)) )
    sys.stdout.write(  "[%s]: DONE: %f\n" % (bmark, (eread_end - eread_start)) )
    # ----------------------------------------
    logger.debug( "[%s]: Reading DGROUPS:" % bmark )
    sys.stdout.write(  "[%s]: Reading DGROUPS:\n" % bmark )
    dgread_start = time.clock()
    abs_filename = os.path.join(cycle_cpp_dir, dgroups_filename)
    dgroups = DeathGroupsReader( abs_filename, logger = logger )
    dgroups.read_dgroup_file( objinfo )
    dgroups.clean_deathgroups()
    dupes = find_dupes( dgroups )
    dgread_end = time.clock()
    logger.debug( "[%s]: DONE: %f" % (bmark, (dgread_end - dgread_start)) )
    sys.stdout.write(  "[%s]: DONE: %f\n" % (bmark, (dgread_end - dgread_start)) )
    # ----------------------------------------
    # for tgt, data in edgeinfo.lastedge_iteritems():
    #     sys.stdout.write(  "%d -> [%d] : %s" % (tgt, data["dtime"], str(data["lastsources"])) )
    ktdict = {}
    debug_count = 0
    debug_tries = 0
    died_at_end_count = 0
    for gnum in dgroups.group2list.keys():
        print "-------[ Group num: %d ]------------------------------------------------" % gnum
        result = get_key_object_types( gnum = gnum,
                                       ktdict = ktdict,
                                       dgroups = dgroups,
                                       edgeinfo = edgeinfo,
                                       objinfo = objinfo,
                                       logger = logger )
        print "-------[ END group num: %d ]--------------------------------------------" % gnum

    # ----------------------------------------
    logger.debug( "[%s]: Total: %d" % (bmark, len(dgroups.group2list)) )
    logger.debug( "[%s]: Tries: %d" % (bmark, debug_tries) )
    logger.debug( "[%s]: Error: %d" % (bmark, debug_count) )
    logger.debug( "[%s]: Died at end: %d" % (bmark, died_at_end_count) )
    sys.stdout.write(  "[%s]: Total: %d\n" % (bmark, len(dgroups.group2list)) )
    sys.stdout.write(  "[%s]: Tries: %d\n" % (bmark, debug_tries) )
    sys.stdout.write(  "[%s]: Error: %d\n" % (bmark, debug_count) )
    sys.stdout.write(  "[%s]: Died at end: %d\n" % (bmark, died_at_end_count) )
    # ----------------------------------------
    # Output target filename
    outfile = os.path.join( workdir, "%s-DGROUPS-TYPES.csv" % bmark )
    with open( outfile, "wb" ) as fptr:
        writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
        writer.writerow( [ "type", "number groups", "maximum", ] )
        for mytype, rec in ktdict.iteritems():
            writer.writerow( [ mytype,
                               rec["total"],
                               rec["max"], ] )
    # Group types output
    outallfile = os.path.join( workdir, "%s-DGROUPS-ALL-TYPES.csv" % bmark )
    with open( outallfile, "wb" ) as fptr:
        writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
        writer.writerow( [ "type", "set_types", "count", ] )
        for mytype, rec in ktdict.iteritems():
            for typeset, count in rec["group_types"].iteritems():
                writer.writerow( [ mytype,
                                   "|".join( [ str(x) for x in typeset ] ),
                                   count ] )
    sys.stdout.write(  "-----[ %s DONE ]---------------------------------------------------------------\n" % bmark )
    logger.debug( "-----[ %s DONE ]---------------------------------------------------------------"
                  % bmark )

def main_process( output = None,
                  main_config = None,
                  benchmark = None,
                  lastedgeflag = False,
                  etanalyze_config = {},
                  global_config = {},
                  edge_config = {},
                  edgeinfo_config = {},
                  objectinfo_config = {},
                  summary_config = {},
                  host_config = {},
                  worklist_config = {},
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO
    print "GLOBAL:"
    pp.pprint(global_config)
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # TODO: Not used? 
    # TODO: work_dir = main_config["directory"]
    # In my config this is: '/data/rveroy/pulsrc/etanalyzer/MYWORK/z-SUMMARY/DGROUPS'
    # Change in basic_merge_summary.ini, under the [dgroups-analyze] section.
    # Setup stdout to file redirect
    thishost = get_actual_hostname( hostname = socket.gethostname().lower(),
                                    host_config = host_config )
    assert( thishost != None )
    thishost = thishost.upper()
    today = date.today()
    today = today.strftime("%Y-%m%d")
    timenow = datetime.now().time().strftime("%H-%M-%S")
    olddir = os.getcwd()
    os.chdir( main_config["output"] )
    workdir = create_work_directory( work_dir = main_config["output"],
                                     thishost = thishost,
                                     today = today,
                                     timenow = timenow,
                                     logger = logger,
                                     interactive = False )
    # TODO What is key -> value in the following dictionaries?
    results = {}
    # TODO What is the results structure?
    # benchark key -> TODO

    # TODO probably need a summary
    # summary = {}

    count = 0
    # Take benchmarks to process from etanalyze_config
    procs = {}
    for bmark, filename in etanalyze_config.iteritems():
        # if skip_benchmark(bmark):
        if ( ((benchmark != "_ALL_") and
              (bmark != benchmark)) or 
             (not check_host( benchmark = bmark,
                              worklist_config = worklist_config,
                              host_config = host_config )) ):
            print "SKIP:", bmark
            continue
        print "=======[ Spawning %s ]================================================" \
            % bmark
        p = Process( target = death_group_analyze,
                     args = ( bmark,
                              cycle_cpp_dir,
                              main_config,
                              filename,
                              objectinfo_config,
                              edge_config,
                              host_config,
                              logger ) )
        p.start()
        procs[bmark] = p
    done = False
    while not done:
        done = True
        for bmark in procs.keys():
            proc = procs[bmark]
            proc.join(60)
            if proc.is_alive():
                done = False
            else:
                del procs[bmark]
    print "DONE."
    os.chdir( olddir )
    exit(0)

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
    parser.add_argument( "--benchmark",
                         dest = "benchmark",
                         help = "Select benchmark.",
                         action = "store" )
    parser.add_argument( "--lastedge",
                         dest = "lastedgeflag",
                         help = "Enable last edge processing.",
                         action = "store_true" )
    parser.add_argument( "--no-lastedge",
                         dest = "lastedgeflag",
                         help = "Disable last edge processing.",
                         action = "store_false" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "dgroup_analyze-MPR.log",
                         debugflag = False,
                         lastedgeflag = False,
                         benchmark = "_ALL_",
                         config = None )
    return parser

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
    etanalyze_config = config_section_map( "etanalyze-output", config_parser )
    main_config = config_section_map( "dgroups-analyze", config_parser )
    # TODO edge_config = config_section_map( "edges", config_parser )
    edge_config = config_section_map( "edgeinfo", config_parser )
    edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    summary_config = config_section_map( "summary_cpp", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    worklist_config = config_section_map( "dgroups-worklist", config_parser )
    return { "global" : global_config,
             "etanalyze" : etanalyze_config,
             "main" : main_config,
             "edge" : edge_config, # TODO is this still needed? TODO
             "edgeinfo" : edgeinfo_config,
             "objectinfo" : objectinfo_config,
             "summary" : summary_config,
             "host" : host_config,
             "worklist" : worklist_config }

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

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    benchmark = args.benchmark
    assert( args.config != None )
    # Get all the configurations. Maybe there's a cleaner way to do this. TODO
    configdict = process_config( args )
    global_config = configdict["global"]
    etanalyze_config = configdict["etanalyze"]
    main_config = configdict["main"]
    edge_config = configdict["edge"]
    edgeinfo_config = configdict["edgeinfo"]
    objectinfo_config = configdict["objectinfo"]
    summary_config = configdict["summary"]
    host_config = process_host_config( configdict["host"] )
    worklist_config = process_worklist_config( configdict["worklist"] )
    print "WORKLIST_CONFIG:"
    pp.pprint(worklist_config)
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         benchmark = args.benchmark,
                         lastedgeflag = args.lastedgeflag,
                         main_config = main_config,
                         etanalyze_config = etanalyze_config,
                         edge_config = edge_config,
                         edgeinfo_config = edgeinfo_config,
                         objectinfo_config = objectinfo_config,
                         summary_config = summary_config,
                         global_config = global_config,
                         host_config = host_config,
                         worklist_config = worklist_config,
                         logger = logger )

if __name__ == "__main__":
    main()
