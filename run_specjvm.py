# run_specjvm.py 
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
import csv
import subprocess
import datetime

from mypytools import mean, stdev, variance

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

def setup_logger( targetdir = ".",
                  filename = "run_specjvm.log",
                  logger_name = 'run_specjvm',
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

def create_work_directory( work_dir, logger = None, interactive = False ):
    os.chdir( work_dir )
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    work_today = "summary-" + today
    if os.path.isfile(work_today):
        print "Can not create %s as directory." % work_today
        exit(11)
    if not os.path.isdir( work_today ):
        os.mkdir( work_today )
    else:
        print "WARNING: %s directory exists." % work_today
        logger.warning( "WARNING: %s directory exists." % work_today )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    return work_today

def main_process( output = None,
                  specjvm_config = None,
                  benchmarks = None,
                  names_config = None,
                  global_config = None,
                  debugflag = False,
                  logger = None ):
    global pp
    pp.pprint( specjvm_config )
    pp.pprint( names_config )
    sdir = global_config["specjvm_dir"]
    cycle_flag = global_config["cycle_flag"]
    simulator = global_config["simulator"]
    t = datetime.date.today()
    datestr = "%d-%02d%02d" % (t.year, t.month, t.day)
    print "TODAY:", datestr
    for bmark in benchmarks:
        tracefile = sdir + specjvm_config[bmark]
        namesfile = sdir + names_config[bmark]
        basename = bmark + "-cpp-" + str(datestr)
        print basename
        # ./simulator xalan.names xalan-cpp-2016-0129 CYCLE
        cmd = [ simulator, namesfile, basename, cycle_flag ]
        fp = get_trace_fp( tracefile, logger )
        sproc = subprocess.Popen( cmd,
                                  stdout = subprocess.PIPE,
                                  stdin = fp,
                                  stderr = subprocess.PIPE )
        result = sproc.communicate()
        for x in result:
            print x
        exit(3333)
    # HERE: TODO
    # 1. Cyclic garbage vs ref count reclaimed:
    #      * Number of objects
    #      * size of objects
    # 2. Number of cycles
    # 3. Size of cycles
    print "GLOBAL:"
    pp.pprint(global_config)
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    work_dir = main_config["directory"]
    results = {}
    count = 0
    work_today = create_work_directory( work_dir, logger = logger )
    olddir = os.getcwd()
    os.chdir( work_today )
    for bmark, filename in etanalyze_config.iteritems():
        # if skip_benchmark(bmark):
        if ( (benchmark != "_ALL_") and (bmark != benchmark) ):
            print "SKIP:", bmark
            continue
        print "=======[ %s ]=========================================================" \
            % bmark
        logger.critical( "=======[ %s ]=========================================================" 
                         % bmark )
        abspath = os.path.join(cycle_cpp_dir, filename)
        if not os.path.isfile(abspath):
            logger.critical("Not such file: %s" % str(abspath))
        else:
            graphs = []
            # Counters TODO: do we need this?
            cycle_total_counter = Counter()
            actual_cycle_counter = Counter()
            cycle_type_counter = Counter()
            logger.critical( "Opening %s." % abspath )
            get_cycles_result = get_cycles_and_edges( abspath )
            cycles = get_cycles_result["cycles"]
            edges = get_cycles_result["edges"]
            # Get edge information
            edge_info_dict = get_cycles_result["edge_info"]
            # Get object dictionary information that has types and sizes
            object_info_dict = get_cycles_result["object_info"]
            total_objects = get_cycles_result["total_objects"]
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
                cycle_info_list = get_cycle_info_list( cycle = cycle,
                                                       objinfo_dict = object_info_dict,
                                                       # objdb,
                                                       logger = logger )
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
                # LIFETIME
                lifetimes = get_lifetimes( G, largest_scc )
                if lastedgeflag:
                    # GET LAST EDGE
                    last_edge = get_last_edge( largest_scc, edge_info_db )
                else:
                    last_edge = None
                debug_lifetimes( G = G,
                                 cycle = cycle,
                                 bmark = bmark, 
                                 logger = logger )
                # -- lifetimes statistics
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
                sizes = get_sizes( G, largest_scc )
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
                                                 objinfo_dict = object_info_dict,
                                                 logger = logger ) 
            print "================================================================================"
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
    parser.set_defaults( logfile = "run_specjvm.log",
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
    specjvm_config = config_section_map( "specjvm", config_parser )
    names_config = config_section_map( "specjvm_names", config_parser )
    benchmarks = config_section_map( "benchmarks", config_parser )
    return ( global_config, specjvm_config, names_config, benchmarks )

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    benchmark = args.benchmark
    assert( args.config != None )
    global_config, specjvm_config, names_config, benchmarks = process_config( args )
    debugflag = global_config["debug"]
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = debugflag  )
    #
    # Main processing
    #
    return main_process( debugflag = debugflag,
                         output = args.output,
                         benchmarks = benchmarks,
                         specjvm_config = specjvm_config,
                         names_config = names_config,
                         global_config = global_config,
                         logger = logger )

if __name__ == "__main__":
    main()
