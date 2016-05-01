# basic_cycle_analyze.py 
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

# TODO from itertools import combinations

from mypytools import mean, stdev, variance
from garbology import EdgeInfoReader, ObjectInfoReader, DeathGroupsReader

pp = pprint.PrettyPrinter( indent = 4 )

ATIME = 0
DTIME = 1
SIZE = 2
TYPE = 3
REASON = 4

def setup_logger( targetdir = ".",
                  filename = "basic_cycle_analyze.log",
                  logger_name = 'basic_cycle_analyze',
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

def DEBUG_types( largest_by_types_with_index, largest_scc ):
    l = largest_by_types_with_index
    if len(largest_scc) == 1:
        print "LEN1: %s <-> %s" % (str(largest_scc), l)
        if l[0][1] == '[B':
            print "DEBUG id: %d" % l[0][0]
    # elif len(largest_scc) == 2:

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
    
def get_types_debug( G, cycle ):
    result = []
    for x in cycle:
        try:
            mynode = G.node[x]
        except:
            print "Unable to get node[ %d ]" % x
            continue
        try:
            mytype = mynode["type"]
        except:
            print "Unable to get type for node[ %d ] -> %s" % (x, str(mynode))
            continue
        result.append(mytype)
    return result

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

def extract_small_cycles( small_summary = None, 
                          bmark = None,
                          objinfo_dict = None,
                          rev_typedict = None,
                          logger = None ):
    global pp
    with open(bmark + "-size1.csv", "wb") as fp1, \
         open(bmark + "-size2.csv", "wb") as fp2, \
         open(bmark + "-size3.csv", "wb") as fp3, \
         open(bmark + "-size4.csv", "wb") as fp4:
        writer = [ None,
                   csv.writer(fp1),
                   csv.writer(fp2),
                   csv.writer(fp3),
                   csv.writer(fp4) ]
        result = [ None, [], [], [], [] ]
        counterlist = {}
        regex = re.compile( "([^\$]+)\$(.*)" )
        total_cycles = 0
        inner_classes_count = Counter()
        # TODO DELETE
        # for feature, fdict in summary.iteritems(): 
        for size, mylist in small_summary.iteritems():
            for cycle in mylist:
                assert( len(cycle) > 0 )
                assert( len(cycle) <= 4 )
                cycle_info_list = []
                for record in cycle:
                    node, saved_type = record
                    try:
                        rec = objinfo_dict[node]
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
                type_tuple = tuple( sorted( [ rev_typedict[x[1]] for x in cycle_info_list ] ) )
                # type_tuple contains all the types in the strongly connected component.
                # This is sorted so that there's a canonical labeling of the type group/tuple.
                assert( len(cycle) == size )
                result[size].append( type_tuple )
                total_cycles += 1
                flag = False
                for tmp in list(type_tuple):
                    if is_inner_class(tmp):
                        inner_classes_count.update( [ tmp ] )
            counterlist[size] = Counter(result[size])
            for row in ( list(key) + [ val ] for key, val
                         in counterlist[size].iteritems() ):
                writer[size].writerow( row )
    pp.pprint( counterlist )
    return { "total_cycles" : total_cycles,
             "inner_classes_count" : inner_classes_count }

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

def output_results( output_path = None,
                    results = None ):
    # Print out results in this format:
    # ========= <- divider
    # benchmark:
    # size, 1, 4, 5, 2, etc
    # largest_cycle, 1, 2, 5, 1, etc
    # number_types, 1, 1, 2, 1, etc
    with open(output_path, "wb") as fp:
        for bmark, infodict in results.iteritems():
            fp.write("================================================================================\n")
            fp.write("%s:\n" % bmark)
            # Totals
            contents = row_to_string( infodict["totals"] )
            fp.write("totals,%s" % contents)
            # Actual largest cycle
            contents = row_to_string( [ len(x) for x in infodict["largest_cycle"] ] )
            fp.write("largest_cycle,%s" % contents)
            # Types 
            contents = row_to_string( [ len(x) for x in infodict["largest_cycle_types_set"] ] )
            fp.write("types,%s" % contents)
    hist_output_base = output_path + "-histogram"
    write_histogram( results = results,
                     tgtbase = hist_output_base,
                     title = "Historgram TODO" )

def output_results_transpose( output_path = None,
                              results = None ):
    # Print out results in this format:
    # ========= <- divider
    # benchmark:
    # size,largest_cycle, number_types, lifetime_ave, lifetime_sd, min, max
    #   10,            5,            2,           22,           5,   2,  50
    for bmark, infodict in results.iteritems():
        bmark_path = bmark + "-" + output_path
        with open(bmark_path, "wb") as fp:
            csvwriter = csv.writer(fp)
            header = [ "totals", "largest_cycle", "num_types",
                       "lifetime_mean", "lifetime_stdev", "liftime_min",
                       "lifetime_max",
                       "size_largest_cycle", "size_all", ]
            csvwriter.writerow( header )
            totals = infodict["totals"]
            largest_cycle = infodict["largest_cycle"]
            types_set = infodict["largest_cycle_types_set"]
            lifetimes = infodict["lifetimes"]
            ltime_mean = infodict["lifetime_mean"]
            ltime_sd = infodict["lifetime_sd"]
            ltime_min = infodict["lifetime_min"]
            ltime_max = infodict["lifetime_max"]
            for i in xrange(len(infodict["totals"])):
                row = [ totals[i], len(largest_cycle[i]),
                        len(types_set[i]), ltime_mean[i],
                        ltime_sd[i], ltime_min[i], ltime_max[i],
                        sum(infodict["sizes_largest_scc"][i]),
                        sum(infodict["sizes_all"][i]), ]
                csvwriter.writerow( row )

def output_summary( output_path = None,
                    summary = None ):
    # Print out results in this format:
    # ========= <- divider
    # benchmark:
    # size,largest_cycle, number_types, lifetime_ave, lifetime_sd, min, max
    #   10,            5,            2,           22,           5,   2,  50
    with open(output_path, "wb") as fp:
        csvwriter = csv.writer(fp)
        header = [ "benchmark", "total_objects", "total_edges", "died_by_heap",
                   "died_by_stack", "died_by_stack_after_heap", "died_by_stack_only",
                   "last_update_null", "number_of_selfloops",
                   "died_by_stack_size", "died_by_heap_size",
                   "last_update_null_heap", "last_update_null_stack", "max_live_size",
                   "last_update_null_size", "last_update_null_heap_size", "last_update_null_stack_size",
                   "died_by_stack_after_heap_size", "died_by_stack_only_size", ]
        csvwriter.writerow( header )
        for bmark, d in summary.iteritems():
            row = [ bmark, d["number_of_objects"], d["number_of_edges"], d["died_by_heap"],
                    d["died_by_stack"], d["died_by_stack_after_heap"], d["died_by_stack_only"],
                    d["last_update_null"], d["number_of_selfloops"],
                    d["size_died_by_stack"], d["size_died_by_heap"],
                    d["last_update_null_heap"], d["last_update_null_stack"], d["max_live_size"],
                    d["last_update_null_size"], d["last_update_null_heap_size"], d["last_update_null_stack_size"],
                    d["died_by_stack_after_heap_size"], d["died_by_stack_only_size"],
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
    os.chdir( today )
    return str(os.getcwd())

def save_interesting_small_cycles( largest_scc, summary ):
    # Interesting is defined to be 4 or smaller
    length = len(largest_scc)
    if length > 0 and length <= 4:
        summary["by_size"][length].append( largest_scc )

def save_largest_cycles( graphlist = None, num = None ):
    largelist = heapq.nlargest( num, graphlist, key = len )
    return largelist

# TODO Do we need selfloops?
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

def find_dupes( dgroups = None):
    count = 0
    dash = 0
    revdict = {}
    dupes = {}
    for objId, group in dgroups.iteritems():
        if count % 1000 == 99:
            sys.stdout.write("-")
            dash += 1
            if dash % 81 == 80:
                sys.stdout.write('\n')
        count += 1
        for mem in group:
            if mem in revdict:
                if mem not in dupes:
                    dupes[mem] = [ revdict[mem], objId ]
                else:
                    dupes[mem].append( objId )
            else:
                revdict[mem] = objId
    sys.stdout.write("\n")
    print "DUPES:"
    for objId, key_list in dupes.iteritems():
        try:
            print "%d -> %d" % (objId, len(key_list))
        except:
            print "ERROR: %s -> %d" % (objId, len(key_list))
    print " -- DUPES DONE."
    return dupes

def main_process( output = None,
                  main_config = None,
                  benchmark = None,
                  lastedgeflag = False,
                  etanalyze_config = None,
                  global_config = None,
                  edge_config = None,
                  edgeinfo_config = None,
                  objectinfo_config = None,
                  summary_config = None,
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
        if ( (benchmark != "_ALL_") and (bmark != benchmark) ):
            print "SKIP:", bmark
            continue
        print "=======[ %s ]=========================================================" \
            % bmark
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
        for tgt, data in edgeinfo.lastedge_iteritems():
            print "%d -> [%d] : %s" % (tgt, data["dtime"], str(data["lastsources"]))
        continue
    print "DONE."
    exit(3333)
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
    parser.set_defaults( logfile = "basic_cycle_analyze.log",
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
    return ( global_config, etanalyze_config, main_config, edge_config,
             edgeinfo_config, objectinfo_config, summary_config )

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

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    benchmark = args.benchmark
    assert( args.config != None )
    global_config, etanalyze_config, main_config, edge_config, \
        edgeinfo_config, objectinfo_config, summary_config  = process_config( args )
    # logging
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
                         logger = logger )

if __name__ == "__main__":
    main()
