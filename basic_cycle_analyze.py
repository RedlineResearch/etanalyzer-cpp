# basic_cycle_analyze.py 
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

from mypytools import mean, stdev, variance

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

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
            print "XXX %s: [ %d - %s ] lifetime: %d" % \
                (bmark, x, n["type"], n["lifetime"])
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

def create_graph( cycle_info_list = None,
                  edgedict = None,
                  logger = None ):
    global pp
    logger.debug( "Creating graph..." )
    g = nx.DiGraph()
    nodeset = set([])
    for mytuple in cycle_info_list:
        node, mytype, mysize, lifetime = mytuple
        nodeset.add(node)
        g.add_node( n = node,
                    type = mytype,
                    lifetime = lifetime )
        if node in edgedict:
            for tgt in edgedict[node]:
                g.add_edge( node, tgt )
    logger.debug( "....done." )
    return g

class ObjDB:
    def __init__( self,
                  objdb1 = None,
                  objdb2 = None,
                  objdb_all = None,
                  debugflag = False,
                  logger = None ):
        self.objdb1 = objdb1
        self.objdb2 = objdb2
        self.objdb_all = objdb_all
        self.sqodb_all = None
        self.alldb = False
        self.sqObj1 = None
        self.sqObj2 = None
        self.logger = logger
        assert( os.path.isfile( objdb_all ) or
                (os.path.isfile( objdb1 ) and os.path.isfile( objdb2 )) )
        if os.path.isfile( objdb_all ):
            try:
                self.sqodb_all = sqorm.Sqorm( tgtpath = objdb_all,
                                              table = "objects",
                                              keyfield = "objId" )
                print "ALL:", objdb_all
                self.alldb = True
                return
            except:
                logger.error( "Unable to load DB ALL file %s" % str(objdb) )
                print "Unable to load DB ALL file %s" % str(objdb)
        if os.path.isfile( objdb1 ):
            try:
                self.sqObj1 = sqorm.Sqorm( tgtpath = objdb1,
                                            table = "objects",
                                            keyfield = "objId" )
            except:
                logger.error( "Unable to load DB 1 file %s" % str(objdb) )
                print "Unable to load DB 1 file %s" % str(objdb)
                assert( False )
        assert(self.sqObj1 != None)
        if os.path.isfile( objdb2 ):
            try:
                self.sqObj2 = sqorm.Sqorm( tgtpath = objdb2,
                                           table = "objects",
                                           keyfield = "objId" )
            except:
                logger.error( "Unable to load DB 2 file %s" % str(objdb) )
                print "Unable to load DB 2 file %s" % str(objdb)
                assert( False )

    def get_record( self, objId ):
        if self.alldb:
            try:
                obj = self.sqodb_all[objId]
                db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
            except:
                # self.logger.error( "Objid [ %s ] not found." % str(objId) )
                return None
        else:
            if objId in self.sqObj1:
                try:
                    obj = self.sqObj1[objId]
                    db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
                except:
                    self.logger.error( "Objid [ %s ] not found in DB1." % str(objId) )
            if objId in self.sqObj2:
                try:
                    obj = self.sqObj1[objId]
                    db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
                except:
                    self.logger.error( "Objid [ %s ] not found in DB2." % str(objId) )
                    # print "Objid [ %s ] not found in DB2." % str(objId)
                    return None
        
        rec = { "objId" : db_objId,
                "type" : db_oType,
                "size" : db_oSize,
                "len" : db_oLen,
                "atime" : db_oAtime,
                "dtime" : db_oDtime,
                "site" : db_oSite }
        return rec

    def get_type( self, objId ):
        db_oType = None
        if self.alldb:
            try:
                obj = self.sqodb_all[objId]
                db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
            except:
                # self.logger.error( "Objid [ %s ] not found." % str(objId) )
                return None
        else:
            if objId in self.sqObj1:
                try:
                    obj = self.sqObj1[objId]
                    db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
                except:
                    self.logger.error( "Objid [ %s ] not found in DB1." % str(objId) )
            if objId in self.sqObj2:
                try:
                    obj = self.sqObj1[objId]
                    db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
                except:
                    self.logger.error( "Objid [ %s ] not found in DB2." % str(objId) )
                    print "Objid [ %s ] not found in DB2." % str(objId)
                    return None
        return db_oType

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

def get_cycles_and_edges( tgtpath ):
    with open(tgtpath) as fp:
        start = False
        cycles = []
        for line in fp:
            line = line.rstrip()
            if line.find("----------") == 0:
                start = not start
                if start:
                    continue
                else:
                    break
            if start:
                line = line.rstrip(",")
                row = line.split(",")
                row = [ int(x) for x in row ]
                cycles.append(row)
        start = False
        edges = set([])
        for line in fp:
            line = line.rstrip()
            if line.find("==========") == 0:
                start = True if not start else False
                if start:
                    continue
                else:
                    break
            if start:
                row = [ int(x) for x in line.split(" -> ") ]
                edges.add(tuple(row))
    edges = set( sorted( list(edges), key = itemgetter(0, 1) ) )
    return (cycles, edges)

def get_cycle_info_list( cycle = None,
                         objdb = None,
                         logger = None ):
    cycle_info_list = []
    for node in cycle:
        try:
            rec = objdb.get_record(node)
            mytype = rec["type"]
            mysize = rec["size"]
            atime = rec["atime"]
            dtime = rec["dtime"]
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

def extract_small_cycles( summary = None, 
                          bmark = None,
                          objdb = None,
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
        inner_classes_cycles = 0
        inner_classes_count = Counter()
        for feature, fdict in summary.iteritems(): 
            for size, mylist in fdict.iteritems():
                for cycle in mylist:
                    assert( len(cycle) > 0 )
                    assert( len(cycle) <= 4 )
                    cycle_info_list = []
                    for record in cycle:
                        node, saved_type = record
                        try:
                            rec = objdb.get_record(node)
                            mytype = rec["type"]
                            mysize = rec["size"]
                            atime = rec["atime"]
                            dtime = rec["dtime"]
                            lifetime = (dtime - atime) if ((dtime > atime) and  (dtime != 0)) \
                                else 0
                            cycle_info_list.append( (node, mytype, mysize, lifetime) )
                        except:
                            logger.critical("Missing node[ %s ]" % str(node))
                            mytype = "<NONE>"
                            mysize = 0
                            lifetime = 0
                            cycle_info_list.append( (node, mytype, mysize, lifetime) )
                    try:
                        assert( saved_type == mytype )
                    except:
                        print " saved [ %s ] <-> from_db [ %s ]" % (saved_type, mytype)
                        exit(10000)
                    type_tuple = tuple( sorted( [ x[1] for x in cycle_info_list ] ) )
                    # type_tuple contains all the types in the strongly connected component.
                    # This is sorted so that there's a canonical labeling of the type group/tuple.
                    assert( len(cycle) == size )
                    result[size].append( type_tuple )
                    total_cycles += 1
                    flag = False
                    for tmp in list(type_tuple):
                        m = regex.match(tmp)
                        if m:
                            if not flag:
                                inner_classes_cycles += 1
                            inner_classes_count.update( [ tmp ] )
                            flag = True
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
                       "lifetime_max", ]
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
                        ltime_sd[i], ltime_min[i], ltime_max[i], ]
                csvwriter.writerow( row )

def create_work_directory( work_dir, interactive = False ):
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

def get_last_edge( largest_scc, edge_info_db ):
    mylist = list(largest_scc)
    for tgt in mylist:
        print "======================================================================"
        print "XXX: %d" % tgt
        result = edge_info_db.get_all( tgt ) # TODO: temporary debug
        print result
    return (0, 0)

def skip_benchmark(bmark):
    return ( bmark == "tradebeans" or # Permanent ignore
             bmark == "tradesoap" or # Permanent ignore
             bmark != "batik"
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

def main_process( output = None,
                  main_config = None,
                  etanalyze_config = None,
                  global_config = None,
                  objdb1_config = None,
                  objdb2_config = None,
                  objdb_ALL_config = None,
                  edge_info_config = None,
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO
    # 1. Cyclic garbage vs ref count reclaimed:
    #      * Number of objects
    #      * size of objects
    # 2. Number of cycles
    # 3. Size of cycles
    print "GLOBAL:"
    pp.pprint(global_config)
    print "EDGE_INFO:"
    pp.pprint(edge_info_config)
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    work_dir = main_config["directory"]
    results = {}
    summary = {}
    count = 0
    today = create_work_directory( work_dir )
    olddir = os.getcwd()
    os.chdir( today )
    for bmark, filename in etanalyze_config.iteritems():
        if skip_benchmark(bmark):
            print "SKIP:", bmark
            continue
        print "=======[ %s ]=========================================================" \
            % bmark
        objdb = setup_objdb( global_config = global_config,
                             objdb1_config = objdb1_config,
                             objdb2_config = objdb2_config,
                             objdb_ALL_config = objdb_ALL_config,
                             benchmark = bmark,
                             logger = logger,
                             debugflag = debugflag )
        edge_info_db = setup_edge_info_db( global_config = global_config,
                                           edge_info_config = edge_info_config,
                                           benchmark = bmark,
                                           logger = logger,
                                           debugflag = debugflag )
        abspath = os.path.join(cycle_cpp_dir, filename)
        if not os.path.isfile(abspath):
            logger.critical("Not such file: %s" % str(abspath))
        else:
            group = 1
            graphs = []
            # Counters TODO: do we need this?
            cycle_total_counter = Counter()
            actual_cycle_counter = Counter()
            cycle_type_counter = Counter()
            cycles, edges = get_cycles_and_edges( abspath )
            selfloops = set()
            edgedict = create_edge_dictionary( edges, selfloops )
            results[bmark] = { "totals" : [],
                               "graph" : [],
                               "largest_cycle" : [],
                               "largest_cycle_types_set" : [],
                               "lifetimes" : [],
                               "lifetime_mean" : [],
                               "lifetime_sd" : [],
                               "lifetime_max" : [],
                               "lifetime_min" : [] }
            summary[bmark] = { "by_size" : { 1 : [], 2 : [], 3 : [], 4 : [] },
                                }
            for index in xrange(len(cycles)):
                cycle = cycles[index]
                cycle_info_list = get_cycle_info_list( cycle, objdb, logger )
                if len(cycle_info_list) == 0:
                    continue
                # GRAPH
                G = create_graph( cycle_info_list = cycle_info_list,
                                  edgedict = edgedict,
                                  logger = logger )
                # Get the actual cycle - LARGEST
                # Sanity check 1: Is it a DAG?
                if nx.is_directed_acyclic_graph(G):
                    logger.warning( "Not a cycle." )
                    logger.warning( "Nodes: %s" % str(G.nodes()) )
                    logger.warning( "Edges: %s" % str(G.edges()) )
                    continue
                ctmplist = list( nx.simple_cycles(G) )
                # Sanity check 2: Check to see it's not empty.
                if len(ctmplist) == 0:
                    # No cycles!!!
                    logger.warning( "Not a cycle." )
                    logger.warning( "Nodes: %s" % str(G.nodes()) )
                    logger.warning( "Edges: %s" % str(G.edges()) )
                    continue
                # TODO TODO TODO
                # Interesting cases are:
                # - largest is size 1 (self-loops)
                # - multiple largest cycles?
                #     * Option 1: choose only one?
                #     * Option 2: ????
                scclist = list(nx.strongly_connected_components(G))
                # Strong connected-ness is a better indication of what we want
                # Unless the cycle is a single node with a self pointer.
                # TOTALS - size of the whole component including leaves
                results[bmark]["totals"].append( len(cycle) )
                cycle_total_counter.update( [ len(cycle) ] )
                # Append graph too
                results[bmark]["graph"].append(G)
                largest_scc = append_largest_SCC( ldict = results[bmark]["largest_cycle"],
                                                  scclist = scclist,
                                                  selfloops = selfloops,
                                                  logger = logger )
                # Cycle length counter
                actual_cycle_counter.update( [ len(largest_scc) ] )
                # Get the types and type statistics
                largest_by_types_with_index = get_types_and_save_index( G, largest_scc )
                largest_by_types = [ x[1] for x in largest_by_types_with_index ]
                largest_by_types_set = set(largest_by_types)
                # DEBUG only: 2015-11-24
                # debug_cycle_algorithms( largest_scc, ctmplist, G )
                # DEBUG_types( largest_by_types_with_index, largest_scc )
                # END DEBUG
                # Save small cycles 
                save_interesting_small_cycles( largest_by_types_with_index, summary[bmark] )
                # TYPE SET
                results[bmark]["largest_cycle_types_set"].append(largest_by_types_set)
                cycle_type_counter.update( [ len(largest_by_types_set) ] )
                group += 1
                # LIFETIME
                lifetimes = get_lifetimes( G, largest_scc )
                # GET LAST EDGE
                last_edge = get_last_edge( largest_scc, edge_info_db )
                debug_lifetimes( G = G,
                                 cycle = cycle,
                                 bmark = bmark, 
                                 logger = logger )
                if len(lifetimes) >= 2:
                    ltimes_mean = mean( lifetimes )
                    ltimes_sd = stdev( lifetimes, ltimes_mean )
                elif len(lifetimes) == 1:
                    ltimes_mean = lifetimes[0]
                    ltimes_sd = 0
                else:
                    raise ValueError("No lifetime == no node found?")
                results[bmark]["lifetimes"].append(lifetimes)
                results[bmark]["lifetime_mean"].append(ltimes_mean)
                results[bmark]["lifetime_sd"].append(ltimes_sd)
                results[bmark]["lifetime_max"].append( max(lifetimes) )
                results[bmark]["lifetime_min"].append( min(lifetimes) )
                # End LIFETIME
                # SIZE PER TYPE COUNT
                # Per bencmark:
                #   count of types -> size in bytes
                #   then group accoring to count of types:
                #         count -> [ size1, s2, s3, ... sn ]
                #   * graph (option 1)
                #   * stats (option 2)
                #   * option3? ? ?
                # End SIZE PER TYPE COUNT
            largelist = save_largest_cycles( results[bmark]["graph"], num = 5 )
            # Make directory and Cd into directory
            if not os.path.isdir(bmark):
                os.mkdir(bmark)
            for_olddir = os.getcwd()
            os.chdir( bmark )
            # Create the CSV files for the data
            small_result = extract_small_cycles( summary = summary[bmark], 
                                                 bmark = bmark,
                                                 objdb = objdb,
                                                 logger = logger ) 
            total_small_cycles = small_result["total_cycles"]
            inner_classes_count = small_result["inner_classes_count"]
            # Cd back into parent directory
            os.chdir( for_olddir )
            print "--------------------------------------------------------------------------------"
            print "num_cycles: %d" % len(cycles)
            print "cycle_total_counter:", str(cycle_total_counter)
            print "actual_cycle_counter:", str(actual_cycle_counter)
            print "cycle_type_counter:", str(cycle_type_counter)
            print "total small cycles:", total_small_cycles
            print "inner_classes_count:", str(inner_classes_count)
            print "--------------------------------------------------------------------------------"
        count += 1
        # if count >= 1:
        #     break
    # benchmark:
    # size, 1, 4, 5, 2, etc
    # largest_cycle, 1, 2, 5, 1, etc
    # number_types, 1, 1, 2, 1, etc
    # TODO - fix this documentation
    print "======================================================================"
    print "===========[ RESULTS ]================================================"
    output_results_transpose( output_path = output,
                              results = results )
    os.chdir( olddir )
    # Print out results in this format:
    print "===========[ SUMMARY ]================================================"
    pp.pprint( summary )
    # TODO: Save the largest X cycles.
    #       This should be done in the loop so to cut down on duplicate work.
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
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "basic_cycle_analyze.log",
                         debugflag = False,
                         benchmark = False,
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
    objdb1_config = config_section_map( "objdb1", config_parser )
    objdb2_config = config_section_map( "objdb2", config_parser )
    objdb_ALL_config = config_section_map( "objdb_ALL", config_parser )
    etanalyze_config = config_section_map( "etanalyze-output", config_parser )
    edge_info_config = config_section_map( "edge_info_DB", config_parser )
    main_config = config_section_map( "cycle-analyze", config_parser )
    return ( global_config,
             objdb1_config, objdb2_config, objdb_ALL_config,
             etanalyze_config, edge_info_config,
             main_config )

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

def setup_edge_info_db( global_config = None,
                        edge_info_config = None,
                        benchmark = None,
                        logger = None,
                        debugflag = False ):
    # set up objdb
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
    global_config, objdb1_config, objdb2_config, objdb_ALL_config, \
        etanalyze_config, edge_info_config, main_config \
        = process_config( args )
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         main_config = main_config,
                         etanalyze_config = etanalyze_config,
                         global_config = global_config,
                         objdb1_config = objdb1_config,
                         objdb2_config = objdb2_config,
                         objdb_ALL_config = objdb_ALL_config,
                         edge_info_config = edge_info_config,
                         logger = logger )

if __name__ == "__main__":
    main()