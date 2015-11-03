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

import mypytools

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
    logger.debug( "Creating graph..." )
    g = nx.DiGraph()
    nodeset = set([])
    for mytuple in cycle_info_list:
        node, mytype, mysize = mytuple
        nodeset.add(node)
        g.add_node( n = node,
                    type = mytype )
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

def get_demographics( cycle ):
    pass

def get_cycle_info_list( cycle = None,
                         objdb = None,
                         logger = None ):
    cycle_info_list = []
    for node in cycle:
        try:
            rec = objdb.get_record(node)
            mytype = rec["type"]
            mysize = rec["size"]
        except:
            logger.critical("Missing node[ %s ]" % str(node))
            mytype = "NONE"
            mysize = 0
        cycle_info_list.append( (node, mytype, mysize) )
    return cycle_info_list

def row_to_string( row ):
    result = None
    strout = StringIO.StringIO()
    csvwriter = csv.writer(strout)
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
        csvw_totals = csv.writer( fp_totals,
                                  quotechar = '"',
                                  quoting = csv.QUOTE_NONNUMERIC )
        csvw_cycles = csv.writer( fp_cycles,
                                  quotechar = '"',
                                  quoting = csv.QUOTE_NONNUMERIC )
        csvw_types = csv.writer( fp_types,
                                 quotechar = '"',
                                 quoting = csv.QUOTE_NONNUMERIC )
        header = [ "benchmark", "total" ]
        csvw_totals.writerow( header )
        csvw_cycles.writerow( header )
        csvw_types.writerow( header )
        dframe = { "totals" : [],
                   "lc" : [],
                   "lc_types" : [] }
        for benchmark, infodict in results.iteritems():
            assert( "totals" in infodict )
            assert( "largest_cycle" in infodict )
            assert( "largest_cycle_types_set" in infodict )
            for total in infodict["totals"]:
                row = [ benchmark, total ]
                dframe["totals"].append(row)
            for cycles in infodict["largest_cycle"]:
                row = [ benchmark, len(cycles) ]
                dframe["lc"].append(row)
            for types in infodict["largest_cycle_types_set"]:
                row = [ benchmark, len(types) ]
                dframe["lc_types"].append(row)
        sorted_totals = sorted( dframe["totals"],
                                key = itemgetter(0) )
        sorted_cycles = sorted( dframe["lc"],
                                key = itemgetter(0) )
        sorted_types = sorted( dframe["lc_types"],
                                key = itemgetter(0) )
        for csvrow in sorted_totals:
            csvw_totals.writerow( csvrow )
        for csvrow in sorted_cycles:
            csvw_cycles.writerow( csvrow )
        for csvrow in sorted_types:
            csvw_types.writerow( csvrow )
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

def create_work_directory( work_dir ):
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
        raw_input("Press ENTER to continue:")
    return today

def skip_benchmark(bmark):
    return ( # bmark == "avrora" or
             # bmark == "batik" or
             # bmark == "eclipse" or
             # bmark == "fop" or
             # bmark == "h2" or
             # bmark == "jython" or
             # bmark == "luindex" or
             # bmark == "lusearch" or
             bmark == "specjbb",
             bmark == "sunflow" )

def main_process( output = None,
                  main_config = None,
                  etanalyze_config = None,
                  global_config = None,
                  objdb1_config = None,
                  objdb2_config = None,
                  objdb_ALL_config = None,
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO
    # 1. Cyclic garbage vs ref count reclaimed:
    #      * Number of objects
    #      * size of objects
    # 2. Number of cycles
    # 3. Size of cycles
    pp.pprint(etanalyze_config)
    pp.pprint(global_config)
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    work_dir = main_config["directory"]
    results = {}
    count = 0
    today = create_work_directory( work_dir )
    olddir = os.getcwd()
    os.chdir( today )
    for bmark, filename in etanalyze_config.iteritems():
        print "Z:", bmark
        # if skip_benchmark(bmark):
        #     continue
        objdb = setup_objdb( global_config = global_config,
                             objdb1_config = objdb1_config,
                             objdb2_config = objdb2_config,
                             objdb_ALL_config = objdb_ALL_config,
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
            edgedict = create_edge_dictionary( edges )
            results[bmark] = { "totals" : [],
                               "graph" : [],
                               "largest_cycle" : [],
                               "largest_cycle_types_set" : [] }
            for cycle in cycles:
                results[bmark]["totals"].append( len(cycle) )
                cycle_total_counter.update( [ len(cycle) ] )
                cycle_info_list = get_cycle_info_list( cycle, objdb, logger )
                G = create_graph( cycle_info_list = cycle_info_list,
                                  edgedict = edgedict,
                                  logger = logger )
                results[bmark]["graph"].append(G)
                # Get the actual cycle
                largest = max(nx.strongly_connected_components(G), key = len)
                results[bmark]["largest_cycle"].append(largest)
                # Cycle length counter
                actual_cycle_counter.update( [ len(largest) ] )
                # Get the types and type statistics
                largest_by_types = get_types( G, largest )
                largest_by_types_set = set(largest_by_types)
                results[bmark]["largest_cycle_types_set"].append(largest_by_types_set)
                cycle_type_counter.update( [ len(largest_by_types_set) ] )
                group += 1
            print "--------------------------------------------------------------------------------"
            print "num_cycles: %d" % len(cycles)
            print "cycle_total_counter:", str(cycle_total_counter)
            print "actual_cycle_counter:", str(actual_cycle_counter)
            print "cycle_type_counter:", str(cycle_type_counter)
        count += 1
        # if count >= 1:
        #     break
    # TODO print "benchmark: %s" % benchmark
    # TODO Where do we need the benchmark?
    # ========= <- divider
    # benchmark:
    # size, 1, 4, 5, 2, etc
    # largest_cycle, 1, 2, 5, 1, etc
    # number_types, 1, 1, 2, 1, etc
    # TODO - fix this documentation
    output_results( output_path = output,
                    results = results )
    os.chdir( olddir )
    # Print out results in this format:
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
    main_config = config_section_map( "cycle-analyze", config_parser )
    return ( global_config,
             objdb1_config, objdb2_config, objdb_ALL_config,
             etanalyze_config,
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

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    benchmark = args.benchmark
    assert( args.config != None )
    global_config, objdb1_config, objdb2_config, objdb_ALL_config, etanalyze_config, main_config \
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
                         logger = logger )

if __name__ == "__main__":
    main()
