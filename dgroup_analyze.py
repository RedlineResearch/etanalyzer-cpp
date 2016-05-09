# dgroup_analyze.py 
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
import networkx as nx
import csv
import subprocess
import datetime
import time
import socket
from collections import defaultdict

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
                  filename = "dgroup_analyze.log",
                  logger_name = 'dgroup_analyze',
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

def create_graph( cycle_info_list = None,
                  edgedict = None,
                  logger = None ):
    global pp
    g = nx.DiGraph()
    nodeset = set([])
    for mytuple in cycle_info_list:
        node, mytype, mysize, lifetime = mytuple
        nodeset.add(node)
        g.add_node( n = node,
                    type = mytype,
                    lifetime = lifetime,
                    size = mysize )
        if node in edgedict:
            for tgt in edgedict[node]:
                g.add_edge( node, tgt )
    return g

def get_types( G, cycle ):
    return [ G.node[x]["type"] for x in cycle ]

def get_types_and_save_index( G, cycle ):
    return [ (x, G.node[x]["type"]) for x in cycle ]

def debug_cycle_algorithms( largest_scc, cyclelist, G ):
    global pp
    print "=================================================="
    other = max( cyclelist, key = len )
    print "SC[ %d ]  SIMP[ %d ]" % (len(largest_scc), len(other))
    if len(largest_scc) == 1:
        node = list(largest_scc)[0]
        if node == 166451:
            print "Found 166451. Writing out graphs in %s" % str(os.getcwd())
            nx.write_gexf( G, "DEBUG-ALL-166451.gexf" )
            nx.write_gexf( G.subgraph( list(largest_scc) ),"DEBUG-SC-166451.gexf" ) 
            print "DONE DEBUG."
            exit(222)
    print "=================================================="
    
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

def get_edges( edgepath ):
    start = False
    done = False
    edges = set([])
    with get_trace_fp(edgepath) as fp:
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
                row = [ int(x) for x in line.split(" -> ") ]
                edges.add(tuple(row))
    assert(done)
    edges = set( sorted( list(edges), key = itemgetter(0, 1) ) )
    return edges

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
    os.chdir( today )
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

def debug_multiple_keys( group = None,
                         key_objects = None,
                         objinfo = None,
                         logger = None ):
    print "-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-"
    print " >>> MULTIPLE KEY DEBUG:"
    print "     [KEYS]"
    for x in key_objects:
        tmp = objinfo.get_record(x)
        print "%d [ %s ][ by %s ] - %d" % \
            (x, objinfo.get_type(x), tmp[ get_index("DIEDBY") ], tmp[ get_index("DTIME") ])
    print "Others:", str( list(set([ objinfo.get_type(x) for x in group if x not in key_objects ])) )
    print "-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-"

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
DIEDBYSTACK = 7
DIEDATEND = 8
NOTFOUND = 9
def get_key_object_types( gnum = None,
                          ktdict = {},
                          dgroups = None,
                          edgeinfo = None,
                          objinfo = None,
                          logger = None ):
    if gnum in dgroups.group2list:
        group = dgroups.group2list[gnum] 
    else:
        return NOTFOUND # TODO What should return be? None?
    # Check if any of the group is a key object
    key_objects = [ x for x in group if objinfo.is_key_object(x) ]
    found_key = False
    used_last_edge = False
    stackflag = objinfo.verify_died_by( grouplist = group,
                                        died_by = "S" )
    # Check to see if the key object is a primitive type array
    if all_primitive_types( group, objinfo ):
        # All are a group unto themselves
        for obj in group:
            tmptype = objinfo.get_type(obj)
            if tmptype in ktdict:
                ktdict[tmptype]["max"] = max( 1, ktdict[tmptype]["max"] )
                ktdict[tmptype]["total"] += 1
            else:
                ktdict[tmptype] = { "total" : 1,
                                   "max" : 1,
                                   "is_array": is_array(tmptype), }
        # print "BY STACK - all primitive" # TODO Make into a logging statement
        return DIEDBYSTACK
    if len(key_objects) > 0:
        # Found key objects
        found_key = True
        if len(key_objects) == 1:
            tgt = key_objects[0]
        else:
            # Multiple keys?
            tgt = None
            debug_multiple_keys( group = group,
                                 key_objects = key_objects,
                                 objinfo = objinfo,
                                 logger = logger )
            return MULTKEY
    else:
        # First try the known groups
        key_objects = fixed_known_key_objects( group = group,
                                               objinfo = objinfo,
                                               logger = logger )
        if key_objects != None:
            tgt = key_objects["obj"]
        else:
            lastrec = get_last_edge_record( group, edgeinfo, objinfo )
            # print "%d @ %d : %d -> %s" % ( gnum,
            #                                lastrec["dtime"],
            #                                lastrec["target"],
            #                                str(lastrec["lastsources"]) )
            if len(lastrec["lastsources"]) == 1:
                # Get the type
                used_last_edge = True
                tgt = lastrec["target"]
            elif len(lastrec["lastsources"]) > 1:
                return NOTFOUND
                # No need to do anything becuase this isn't a key object?
                # But DO we need to update the counts of the death groups above TODO
            elif len(lastrec["lastsources"]) == 0:
                # Means stack object?
                if not stackflag:
                    print "-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-"
                    print "No last edge but group didn't die by stack as a whole:"
                    for obj in group:
                        rec = objinfo.get_record( obj )
                        print "[%d] : %s -> %s" % ( obj,
                                                    rec[ get_index("TYPE") ],
                                                    rec[ get_index("DIEDBY") ] )
                    print "-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-"
                else:
                    # Died by stack. Each object is its own key object
                    for obj in group:
                        tmptype = objinfo.get_type(obj)
                        if tmptype in ktdict:
                            ktdict[tmptype]["max"] = max( 1, ktdict[tmptype]["max"] )
                            ktdict[tmptype]["total"] += 1
                        else:
                            is_array_flag = is_array(tmptype)
                            ktdict[tmptype] = { "total" : 1,
                                               "max" : 1,
                                               "is_array": is_array_flag, }
                    return DIEDBYSTACK
            if objinfo.died_at_end(tgt):
                return DIEDATEND
    mytype = objinfo.get_type(tgt)
    is_array_flag = is_array(mytype)
    group_types = [ objinfo.get_type(x) for x in group if x != tgt ]
    print "-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
    print "%s:" % mytype
    if is_array(mytype) and len(group_types) > 0:
        print " --- DEBUG:"
        print "%d [ %s ] - %d" % (tgt, objinfo.get_type(tgt), objinfo.get_death_time(tgt))
        for x in group:
            tmptype = objinfo.get_type(x)
            if x != tgt:
                tmp = objinfo.get_record(x)
                print "%d [ %s ][ by %s ] - %d" % \
                    (x, objinfo.get_type(x), tmp[ get_index("DIEDBY") ], tmp[ get_index("DTIME") ])
    else:
        for t in group_types:
            print t,
    print
    print "-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
    if mytype in ktdict:
        if stackflag and is_array_flag:
            keysrc = "WITH KEY" if found_key else ("LAST EDGE" if used_last_edge else "??????")
            print "BY STACK %s:" % keysrc
            for obj in group:
                tmptype = objinfo.get_type(obj)
                if tmptype in ktdict:
                    ktdict[tmptype]["max"] = max( 1, ktdict[tmptype]["max"] )
                    ktdict[tmptype]["total"] += 1
                else:
                    ktdict[tmptype] = { "total" : 1,
                                       "max" : 1,
                                       "is_array": is_array(tmptype), }
            return DIEDBYSTACK
        ktdict[mytype]["max"] = max( len(group), ktdict[mytype]["max"] )
        ktdict[mytype]["total"] += 1
    else:
        ktdict[mytype] = { "total" : 1,
                           "max" : len(group),
                           "is_array": is_array_flag, }
    return ONEKEY

def check_host( benchmark = None,
                worklist_config = {},
                host_config = {} ):
    thishost = socket.gethostname()
    for wanthost in worklist_config[benchmark]:
        if thishost in host_config[wanthost]:
            return True
    return False

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
    work_dir = main_config["directory"]
    # In my config this is: '/data/rveroy/pulsrc/etanalyzer/MYWORK/z-SUMMARY/DGROUPS'
    # Change in basic_merge_summary.ini, under the [dgroups-analyze] section.

    # TODO What is key -> value in the following dictionaries?
    results = {}
    # TODO What is the results structure?
    # benchark key -> TODO

    # TODO probably need a summary
    # summary = {}

    olddir = os.getcwd()
    count = 0
    today = create_work_directory( work_dir,
                                   logger = logger )
    os.chdir( today )
    # Take benchmarks to process from etanalyze_config
    for bmark, filename in etanalyze_config.iteritems():
        # if skip_benchmark(bmark):
        if ( ((benchmark != "_ALL_") and
               (bmark != benchmark)) or 
             (not check_host( benchmark = bmark,
                              worklist_config = worklist_config,
                              host_config = host_config )) ):
            print "SKIP:", bmark
        print "=======[ %s ]=========================================================" \
            % bmark
        sys.stdout.flush()
        # Get object dictionary information that has types and sizes
        # TODO Put this code into garbology.py
        typedict = {}
        rev_typedict = {}
        objectinfo_path = os.path.join(cycle_cpp_dir, objectinfo_config[bmark])
        objinfo = ObjectInfoReader( objectinfo_path, logger = logger )
        # ----------------------------------------
        print "Reading OBJECTINFO file:",
        oread_start = time.clock()
        objinfo.read_objinfo_file()
        oread_end = time.clock()
        print " - DONE: %f" % (oread_end - oread_start)
        objinfo.print_out( numlines = 20 )
        # ----------------------------------------
        print "Reading EDGEINFO file:",
        edgeinfo_path = os.path.join( cycle_cpp_dir, edge_config[bmark] )
        edgeinfo = EdgeInfoReader( edgeinfo_path, logger = logger )
        eread_start = time.clock()
        edgeinfo.read_edgeinfo_file()
        eread_end = time.clock()
        print " - DONE: %f" % (eread_end - eread_start)
        edgeinfo.print_out( numlines = 30 )
        print "Reading DGROUPS:",
        abs_filename = os.path.join(cycle_cpp_dir, filename)
        print "Open: %s" % abs_filename
        dgroups = DeathGroupsReader( abs_filename, logger = logger )
        dgroups.read_dgroup_file( objinfo )
        dgroups.clean_deathgroups()
        dupes = find_dupes( dgroups )
        # for tgt, data in edgeinfo.lastedge_iteritems():
        #     print "%d -> [%d] : %s" % (tgt, data["dtime"], str(data["lastsources"]))
        ktdict = {}
        debug_count = 0
        debug_tries = 0
        died_at_end_count = 0
        for gnum in dgroups.group2list.keys():
            result = get_key_object_types( gnum = gnum,
                                           ktdict = ktdict,
                                           dgroups = dgroups,
                                           edgeinfo = edgeinfo,
                                           objinfo = objinfo,
                                           logger = logger )

        print "Total: %d" % len(dgroups.group2list)
        print "Tries: %d" % debug_tries
        print "Error: %d" % debug_count
        print "Died at end: %d" % died_at_end_count
        print "==============================================================================="
        for mytype, rec in ktdict.iteritems():
            print "%s,%d,%d" % ( mytype, rec["total"], rec["max"], )
        print "==============================================================================="
        sys.stdout.flush()
    print "DONE."
    exit(3333)
    # TODO Delete after this
    # benchmark:
    # size, 1, 4, 5, 2, etc
    # largest_cycle, 1, 2, 5, 1, etc
    # number_types, 1, 1, 2, 1, etc
    # TODO - fix this documentation
    print "======================================================================"
    print "===========[ RESULTS ]================================================"
    output_results_transpose( output_path = output,
                              results = results )
    output_summary( output_path = output,
                    summary = summary )
    os.chdir( olddir )
    # Print out results in this format:
    print "===========[ SUMMARY ]================================================"
    print_summary( summary )
    # TODO: Save the largest X cycles.
    #       This should be done in the loop so to cut down on duplicate work.
    print "===========[ TYPES ]=================================================="
    benchmarks = summary.keys()
    pp.pprint(benchmarks)
    # TODO
    print "---------------[ Common to ALL ]--------------------------------------"
    # common_all = set.intersection( *[ set(summary[b]["types"].keys()) for b in benchmarks ] )
    # common_all = [ rev_typedict[x] for x in common_all ]
    # pp.pprint( common_all )
    # print "---------------[ Counter over all benchmarks ]------------------------"
    # g_types = Counter()
    # for bmark, bdict in summary.iteritems():
    #     g_types.update( bdict["types"] )
    # for key, value in g_types.iteritems():
    #     print "%s: %d" % (rev_typedict[key], value)
    # print "Number of types - global: %d" % len(g_types)
    print "===========[ DONE ]==================================================="
    exit(1000)

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
    parser.set_defaults( logfile = "dgroup_analyze.log",
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

# TODO: TO REMOVE 8 jan 2016
def setup_objdb( global_config = None,
                 objdb1_config = None,
                 objdb2_config = None,
                 objdb_ALL_config = None,
                 benchmark = None,
                 logger = None,
                 debugflag = False ):
    # set up objdb
    objdb1 = os.path.join( global_config["objdb_dir"], objdb1_config[benchmark] )
    objdb2 = os.path.join( global_config["objdb_dir"], objdb2_config[benchmark] )
    objdb_all = os.path.join( global_config["objdb_dir"], objdb_ALL_config[benchmark] )
    return ObjDB( objdb1 = objdb1,
                  objdb2 = objdb2,
                  objdb_all = objdb_all,
                  debugflag = debugflag,
                  logger = logger )

# TODO: TO REMOVE 8 jan 2016
def setup_edge_info_db( global_config = None,
                        edge_info_config = None,
                        benchmark = None,
                        logger = None,
                        debugflag = False ):
    # set up edge_info_db
    tgtpath = os.path.join( global_config["edge_info_dir"], edge_info_config[benchmark] )
    print tgtpath
    try:
        edge_info_db = sqorm.Sqorm( tgtpath = tgtpath,
                                    table = "edges",
                                    keyfield = "tgtId" )
    except:
        logger.error( "Unable to load edge info DB: %s" % str(tgtpath) )
        print "Unable to load edge info DB: %s" % str(tgtpath)
        assert( False )
    return edge_info_db

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
