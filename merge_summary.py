# merge_summary.py 
#
import argparse
import os
import sys
import time
import logging
import sqorm
import cPickle
import pprint
import re
import ConfigParser
from operator import itemgetter
from collections import Counter
import networkx as nx
import StringIO
import csv
import subprocess
import datetime
import heapq
import tarfile
from tempfile import mkdtemp
from itertools import combinations
from shutil import move, rmtree
from glob import glob

from mypytools import mean, stdev, variance

from garbology import SummaryReader

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

ATIME = 0
DTIME = 1
SIZE = 2
TYPE = 3
REASON = 4

def setup_logger( targetdir = ".",
                  filename = "merge_summary.log",
                  logger_name = 'merge_summary',
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

def create_edge_dictionary( edges = None,
                            selfloops = None ):
    edgedict = {}
    for edge in edges:
        src = edge[0]
        tgt = edge[1]
        if src == tgt:
            selfloops.add( src )
        if src not in edgedict:
            edgedict[src] = [ tgt ]
        else:
            edgedict[src].append(tgt)
    for src, tgtlist in edgedict.iteritems():
        edgedict[src] = set(tgtlist)
    return edgedict

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
                    print "START--"
                    continue
                else:
                    print "--DONE"
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

def get_typeId( mytype, typedict, rev_typedict ):
    if mytype in typedict:
        return typedict[mytype]
    else:
        lastkey = len(typedict.keys())
        typedict[mytype] = lastkey + 1
        rev_typedict[lastkey + 1] = mytype
        return lastkey + 1

# Input: objectinfo_path that points to the object information
# Output:
#    typedict: typeId -> actual type
#    rev_typedict:  actual type -> typeId
def get_object_info( objectinfo_path, typedict, rev_typedict ):
    start = False
    done = False
    object_info = {}
    with open(objectinfo_path) as fp:
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
                rowtmp = line.split(",")
                row = [ int(x) for x in rowtmp[1:4] ]
                mytype = rowtmp[-2]
                row.append( get_typeId( mytype, typedict, rev_typedict ) )
                row.append( rowtmp[-1] )
                object_info[int(rowtmp[0])] = tuple(row)
    assert(done)
    return object_info

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

def render_graphs( rscript_path = None,
                   barplot_script = None,
                   csvfile = None,
                   graph_dir = None,
                   logger = None,
                   debugflag = False ):
    curdir = os.getcwd()
    csvfile_abs = os.path.join( curdir, csvfile )
    assert( os.path.isfile( rscript_path ) )
    assert( os.path.isfile( barplot_script ) )
    # TODO print csvfile
    # TODO print csvfile_abs
    assert( os.path.isfile( csvfile_abs ) )
    assert( os.path.isdir( graph_dir ) )
    cmd = [ rscript_path, # The Rscript executable
            barplot_script, # Our R script that generates the plots/graphs
            csvfile_abs, # The csv file that contains the data
            graph_dir, ] # Where to place the PDF output files
    print "Running R barplot script  on %s -> directory %s" % (csvfile, graph_dir)
    logger.debug( "[ %s ]" % str(cmd) )
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    if debugflag:
        logger.debug("--------------------------------------------------------------------------------")
        for x in result:
            logger.debug(str(x))
            print "XXX:", str(x)
        logger.debug("--------------------------------------------------------------------------------")
    # TODO: Parse the result to figure out if it went wrong.

def output_R( benchmark = None ):
    pass
    # Need benchmark.
    # TODO: Do we need this?
    # Call tables.R perhaps?

# Outputs all the benchmarks and the related information
def output_summary( output_path = None,
                    summary = None ):
    # Print out results in this format:
    # ========= <- divider
    # benchmark:
    # size,largest_cycle, number_types, lifetime_ave, lifetime_sd, min, max
    #   10,            5,            2,           22,           5,   2,  50
    # TODO: This documentation seems wrong. TODO
    with open(output_path, "wb") as fp:
        csvwriter = csv.writer(fp)
        header = [ "benchmark", "total_objects", "total_edges",
                   "died_by_heap", "died_by_stack", "died_at_end",
                   "died_by_stack_after_heap", "died_by_stack_only",
                   "last_update_null",
                   "died_by_stack_size", "died_by_heap_size", "died_at_end_size",
                   "max_live_size",
                   "died_by_stack_after_heap_size", "died_by_stack_only_size",
                   # "last_update_null_heap", "last_update_null_stack", 
                   # "last_update_null_size", "last_update_null_heap_size", "last_update_null_stack_size",
                   ]
        csvwriter.writerow( header )
        for bmark, d in summary.iteritems():
            row = [ bmark, d["number_of_objects"], d["number_of_edges"],
                    d["died_by_heap"], d["died_by_stack"], d["died_at_end"],
                    d["died_by_stack_after_heap"], d["died_by_stack_only"],
                    d["last_update_null"],
                    d["died_by_stack_size"], d["died_by_heap_size"], d["died_at_end_size"],
                    d["max_live_size"],
                    d["died_by_stack_after_heap_size"], d["died_by_stack_only_size"],
                    # d["last_update_null_heap"], d["last_update_null_stack"], 
                    # d["last_update_null_size"], d["last_update_null_heap_size"], d["last_update_null_stack_size"],
                    ]
            csvwriter.writerow( row )

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

def save_interesting_small_cycles( largest_scc, summary ):
    # Interesting is defined to be 4 or smaller
    length = len(largest_scc)
    if length > 0 and length <= 4:
        summary["by_size"][length].append( largest_scc )

def save_largest_cycles( graphlist = None, num = None ):
    largelist = heapq.nlargest( num, graphlist, key = len )
    return largelist

def append_largest_SCC( ldict = None,
                        scclist = None,
                        selfloops = None,
                        logger = None ):
    maxscc_len = max( ( len(x) for x in scclist ) )
    if maxscc_len == 1:
        # When the largest strongly connected component is a single node,
        # We can't use the largest, because all nodes will be a SCC.
        # We instead have to use the selfloops
        selfies = set()
        for cycle in scclist:
            cycle = list(cycle)
            node = cycle[0]
            if node in selfloops:
                selfies.add( node )
        assert( len(selfies) > 0 )
        if len(selfies) > 1:
            logger.critical( "More than one selfie in list: %s" % str(selfies) )
        largest_scc = [ selfies.pop() ]
    else:
        largest_scc = max( scclist, key = len )
    ldict.append(largest_scc)
    return largest_scc

def get_last_edge_from_result( edge_list ):
    ledge = edge_list[0]
    latest = ledge[4]
    for newedge in edge_list[1:]:
        if newedge[4] > latest:
            ledge = newedge
    return ledge

def get_last_edge( largest_scc, edge_info_db ):
    mylist = list(largest_scc)
    print "======================================================================"
    print mylist
    print "----"
    last_edge_list = []
    for tgt in mylist:
        try:
            result = edge_info_db.get_all( tgt ) # TODO: temporary debug
            print "XXX: %d" % tgt
        except KeyError:
            result = []
            print "ZZZ: %d" % tgt
        print result
        # The edge tuple is:
        # (tgtId, srcId, fieldId, alloc time, death time )
        # => Get the edge with the latest death time whose source ID isn't in
        #    the cycle.
        last_edge = get_last_edge_from_result( result )
        last_edge_list.append( last_edge )
    print "====[ END ]==========================================================="
    last_edge = get_last_edge_from_result( last_edge_list )
    return (last_edge[1], last_edge[0])

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

def summary_by_size( objinfo = None,
                     cycles = None,
                     typedict = None,
                     summary = None,
                     logger = None ):
    print summary.keys()
    sbysize = summary["sbysize"]
    exit(1000)
    dbh = 0
    dbs = 0
    total_size = 0
    # TODO
    dbs_after_heap = 0
    dbs_only = 0
    last_update_null = 0
    # END TODO
    tmp = 0
    for cycle in cycles:
        for c in cycle:
            mysize = objinfo[c][SIZE]
            total_size += mysize
            reason = objinfo[c][REASON]
            if reason == "S":
                dbs += mysize
            elif reason == "H":
                dbh += mysize
    sbysize = { "died_by_heap" : dbh, # size
                "died_by_stack" : dbs, # size
                "died_by_stack_after_heap" : dbs_after_heap, # subset of died_by_stack TODO
                "died_by_stack_only" : dbs_only, # subset of died_by_stack TODO
                "last_update_null" : last_update_null, # subset of died_by_heap TODO
                "size" : total_size, }
    return sbysize

def skip_file( fname = None ):
    return ( (fname == "docopy.sh") or
             (fname == "email.sh") or
             (fname == "README.txt") or
             os.path.isdir( fname ) )

def backup_old_graphs( graph_dir_path = None,
                       backup_graph_dir_path = None,
                       base_temp_dir = None,
                       pdfs_config = None,
                       workdir = None ):
    assert( os.path.isdir( backup_graph_dir_path ) )
    assert( os.path.isdir( graph_dir_path ) )
    temp_dir = mkdtemp( dir = base_temp_dir )
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    print "Using temporary directory: %s" % temp_dir
    print "BACKUP graph dir: %s" % backup_graph_dir_path
    print "today is: %s" % str(today)
    # tar and bzip2 -9 all old files to today.tar
    # Move all files to TEMP_DIR
    tfilename_base = os.path.join( backup_graph_dir_path, "object_barplots-" + today + "*" )
    flist = glob( tfilename_base )
    i = len(flist) + 1
    tempbase = "object_barplots-%s-%s.tar" % (today, str(i))
    tfilename = os.path.join( temp_dir, tempbase )
    assert( not os.path.exists( tfilename ) )
    tarfp = tarfile.open( tfilename, mode = 'a' )
    print "Taring to: %s" % tfilename
    os.chdir( graph_dir_path )
    flist = []
    for fname in os.listdir( graph_dir_path ):
        if ( skip_file( fname ) or
             os.path.samefile( tfilename, fname ) ):
            continue
        flist.append( fname )
    if len(flist) == 0:
        print "Empty source graph directory: %s" % graph_dir_path
        print " - attempting to remove %s" % temp_dir
        rmtree( temp_dir )
        return
    for fname in flist:
        if os.path.isfile( fname ):
            print "  adding %s..." % fname
            tarfp.add( fname )
    tarfp.close()
    for fname in flist:
        if os.path.isfile( fname ):
            print "  deleting %s." % fname
            os.remove( fname )
    cmd = [ "bzip2", "-9", tfilename ]
    print "Bzipping...",
    print "CMD: %s" % cmd
    print "Check file:", os.path.isfile( tfilename )
    proc = subprocess.Popen( cmd,
                             stdout = subprocess.PIPE,
                             stderr = subprocess.PIPE )
    result = proc.communicate()
    # TODO Check result?
    print "BZIP2 DONE."
    bz2filename = tfilename + ".bz2"
    print "--------------------------------------------------------------------------------"
    assert( os.path.isfile(bz2filename) )
    # Check to see if the target exists already.
    tgtfile = os.path.join( backup_graph_dir_path, bz2filename )
    print "Moving: %s --> %s" % (bz2filename, backup_graph_dir_path)
    move( bz2filename, backup_graph_dir_path )
    print "Attempting to remove %s" % temp_dir
    rmtree( temp_dir )

def main_process( output = None,
                  main_config = None,
                  lastedgeflag = False,
                  etanalyze_config = None,
                  worklist_config = None,
                  global_config = None,
                  edge_config = None,
                  edgeinfo_config = None,
                  objectinfo_config = None,
                  summary_config = None,
                  pdfs_config = None,
                  dgroups2db_config = None,
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO
    # 1. Cyclic garbage vs ref count reclaimed:
    #      * Number of objects
    #      * size of objects
    # 2. Number of cycles
    # 3. Size of cycles
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    graph_dir_path = global_config["graph_dir"]
    backup_graph_dir_path = global_config["backup_graph_dir"]
    temp_dir = global_config["temp_dir"]
    work_dir = main_config["directory"]
    results = {}
    summary = {}
    typedict = {} # Type dictionary is ACROSS all benchmarks
    rev_typedict = {} # Type dictionary is ACROSS all benchmarks
    count = 0
    olddir = os.getcwd()
    # TODO today = create_work_directory( work_dir, logger = logger )
    # TODO os.chdir( today )
    workdir = dgroups2db_config["output"]
    for bmark in worklist_config.keys():
        # TODO: Add check host functionality here
        # if bmark != "tomcat":
        #     print bmark
        #     continue
        abspath = os.path.join( workdir,
                                bmark + dgroups2db_config["file-dgroups"] )
        print "=======[ %s ]=========================================================" \
            % bmark
        logger.critical( "=======[ %s ]=========================================================" 
                         % bmark )
        if os.path.isfile(abspath):
            #----------------------------------------------------------------------
            #      SETUP
            #----------------------------------------------------------------------
            group = 1
            graphs = []
            # Counters TODO: do we need this?
            cycle_total_counter = Counter()
            actual_cycle_counter = Counter() # TODO
            cycle_type_counter = Counter() # TODO
            logger.critical( "Opening %s." % abspath )
            #----------------------------------------------------------------------
            #      SUMMARY
            #----------------------------------------------------------------------
            # Get summary
            summary_path = os.path.join(cycle_cpp_dir, summary_config[bmark])
            # summary_sim = get_summary( summary_path )
            sreader = SummaryReader( summary_file = summary_path,
                                       logger = logger )
            sreader.read_summary_file()
            #     get summary by size
            number_of_objects = sreader.get_number_of_objects()
            number_of_edges = sreader.get_number_of_edges()
            died_by_stack = sreader.get_number_died_by_stack()
            died_by_heap = sreader.get_number_died_by_heap()
            died_at_end = sreader.get_number_died_at_end()
            died_by_stack_after_heap = sreader.get_number_died_by_stack_after_heap()
            died_by_stack_only = sreader.get_number_died_by_stack_only()
            died_by_stack_after_heap_size = sreader.get_size_died_by_stack_after_heap()
            died_by_stack_only_size = sreader.get_size_died_by_stack_only()
            died_by_stack_size = sreader.get_size_died_by_stack()
            died_by_heap_size = sreader.get_size_died_by_heap()
            died_at_end_size = sreader.get_size_died_at_end()

            last_update_null = sreader.get_last_update_null()

            max_live_size = sreader.get_max_live_size()
            final_time = sreader.get_final_garbology_time()
            # last_update_null_heap = summary_sim["last_update_null_heap"]
            # last_update_null_stack = summary_sim["last_update_null_stack"]
            # last_update_null_size = summary_sim["last_update_null_size"]
            # last_update_null_heap_size = summary_sim["last_update_null_heap_size"]
            # last_update_null_stack_size = summary_sim["last_update_null_stack_size"]
            summary[bmark] = { "died_by_heap" : died_by_heap, # total of
                               "died_by_stack" : died_by_stack, # total of
                               "died_at_end" : died_at_end, # total of
                               "died_by_stack_after_heap" : died_by_stack_after_heap, # subset of died_by_stack
                               "died_by_stack_only" : died_by_stack_only, # subset of died_by_stack
                               "died_by_stack_after_heap_size" : died_by_stack_after_heap_size, # size of
                               "died_by_stack_only_size" : died_by_stack_only_size, # size of
                               "last_update_null" : last_update_null, # subset of died_by_heap
                               "max_live_size" : max_live_size,
                               "number_of_objects" : number_of_objects,
                               "number_of_edges" : number_of_edges,
                               "types" : Counter(), # counts of types using type IDs
                               "died_by_stack_size" : died_by_stack_size, # size, not object count
                               "died_by_heap_size" : died_by_heap_size, # size, not object count
                               "died_at_end_size" : died_at_end_size, # size, not object count
                               # TODO "last_update_null_heap" : last_update_null_heap, # subset of died_by_heap
                               # TODO "last_update_null_stack" : last_update_null_stack, # subset of died_by_heap
                               # TODO "last_update_null_size" : last_update_null_size, # size of
                               # TODO "last_update_null_heap_size" : last_update_null_heap_size, # size of
                               # TODO "last_update_null_stack_size" : last_update_null_stack_size, # size of
                               }
            #----------------------------------------------------------------------
            #      CYCLES
            #----------------------------------------------------------------------
            # Get object dictionary information that has types and sizes
            # TODO objectinfo_path = os.path.join(cycle_cpp_dir, objectinfo_config[bmark])
            # TODO object_info_dict = get_object_info( objectinfo_path, typedict, rev_typedict )
            print "--------------------------------------------------------------------------------"
        else:
            logger.critical("Not such file: %s" % str(abspath))
        count += 1
        # if count >= 1:
        #     break
    print "======================================================================"
    print "===========[ SUMMARY ]================================================"
    output_summary( output_path = output,
                    summary = summary )
    old_dir = os.getcwd()
    backup_old_graphs( graph_dir_path = graph_dir_path,
                       pdfs_config = pdfs_config,
                       backup_graph_dir_path = backup_graph_dir_path,
                       base_temp_dir = temp_dir,
                       workdir = workdir )
    os.chdir( old_dir )
    # run object_barplot.R
    render_graphs( rscript_path = global_config["rscript_path"],
                   barplot_script = global_config["barplot_script"],
                   csvfile = output, # csvfile is the input from the output_summary earlier 
                   graph_dir = global_config["graph_dir"],
                   logger = logger,
                   debugflag = debugflag )
    #=====[ DONE ]=============================================================
    os.chdir( olddir )
    # Print out results in this format:
    print_summary( summary )
    # TODO: Save the largest X cycles.
    #       This should be done in the loop so to cut down on duplicate work.
    # TODO
    # print "===========[ TYPES ]=================================================="
    # benchmarks = summary.keys()
    # print "---------------[ Common to ALL ]--------------------------------------"
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
    parser.set_defaults( logfile = "merge_summary.log",
                         debugflag = False,
                         lastedgeflag = False,
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
    # So not sure why the main config section is "cycle-analyze" here. -RLV
    main_config = config_section_map( "cycle-analyze", config_parser )
    worklist_config = config_section_map( "merge-summary-worklist", config_parser )
    # TODO edge_config = config_section_map( "edges", config_parser )
    edge_config = config_section_map( "edgeinfo", config_parser )
    edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    summary_config = config_section_map( "summary-cpp", config_parser )
    pdfs_config = config_section_map( "pdfs", config_parser )
    dgroups2db_config = config_section_map( "dgroups2db", config_parser )
    return { "global_config" : global_config,
             "etanalyze_config" : etanalyze_config,
             "main_config" : main_config,
             "edge_config" : edge_config,
             "edgeinfo_config" : edgeinfo_config,
             "objectinfo_config" : objectinfo_config,
             "summary_config" : summary_config,
             "pdfs_config" : pdfs_config,
             "dgroups2db_config" : dgroups2db_config,
             "worklist_config" : worklist_config, }

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    # TODO benchmark = args.benchmark
    assert( args.config != None )
    result = process_config( args ) 
    global_config = result["global_config"]
    etanalyze_config = result["etanalyze_config"]
    main_config = result["main_config"]
    edge_config = result["edge_config"]
    edgeinfo_config = result["edgeinfo_config"]
    objectinfo_config = result["objectinfo_config"]
    summary_config = result["summary_config"]
    pdfs_config = result["pdfs_config"]
    dgroups2db_config = result["dgroups2db_config"]
    worklist_config = result["worklist_config"]
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         lastedgeflag = args.lastedgeflag,
                         worklist_config = worklist_config,
                         main_config = main_config,
                         etanalyze_config = etanalyze_config,
                         edge_config = edge_config,
                         edgeinfo_config = edgeinfo_config,
                         objectinfo_config = objectinfo_config,
                         summary_config = summary_config,
                         global_config = global_config,
                         dgroups2db_config = dgroups2db_config,
                         pdfs_config = pdfs_config,
                         logger = logger )

if __name__ == "__main__":
    main()
