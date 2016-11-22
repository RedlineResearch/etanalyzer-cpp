# run_simulator.py 
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
import datetime
from collections import defaultdict

import subprocess
from multiprocessing import Process
import socket
import shutil

from mypytools import mean, stdev, variance, \
    is_specjvm, is_dacapo, is_minibench

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

def setup_logger( targetdir = ".",
                  filename = "run_simulator.log",
                  logger_name = 'run_simulator',
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
    print "X:", work_dir
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    work_today = "run-" + today
    print " :", work_today
    if os.path.isfile(work_today):
        print "Can not create %s as directory." % work_today
        exit(11)
    if not os.path.isdir( work_today ):
        print "Making:", work_today
        os.mkdir( work_today )
    else:
        print "WARNING: %s directory exists." % work_today
        logger.warning( "WARNING: %s directory exists." % work_today )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    return work_today

def backup_old_simulator_output( cycle_cpp_dir, backup_cycle_cpp_dir ):
    # For every file (not a directory in cycle_cpp_dir,
    # move to backup_cycle_cpp_dir.
    assert( os.path.isdir( backup_cycle_cpp_dir ) )
    for fname in os.listdir( cycle_cpp_dir ):
        abs_fname = os.path.join( cycle_cpp_dir, fname )
        if os.path.isfile(abs_fname):
            # Move this file into backup directory
            tgtfile = os.path.join( backup_cycle_cpp_dir, fname )
            if os.path.isfile( tgtfile ):
                os.remove( tgtfile )
            shutil.move( abs_fname, backup_cycle_cpp_dir )


def run_subprocess( cmd = None,
                    stdout = None,
                    stdin = None,
                    stderr = None,
                    cwd = None ):
    sproc = subprocess.Popen( cmd,
                              stdout = stdout,
                              stdin = stdin,
                              stderr = stderr,
                              cwd = cwd )
    sproc.communicate()

def check_host( benchmark = None,
                hostlist= {},
                host_config = {} ):
    thishost = socket.gethostname()
    print "thishost:", thishost
    print "hostlist:", str(hostlist)
    print "host_config:", host_config
    for wanthost in hostlist:
        print "want:", wanthost
        if thishost in host_config[wanthost]:
            return True
    print "FALSE"
    return False

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


def main_process( output = None,
                  benchmarks = None,
                  worklist = {},
                  global_config = {},
                  bmark_config = {},
                  names_config = {},
                  simulator_config = {},
                  host_config = {},
                  mainfunction_config = {},
                  debugflag = None,
                  logger = None ):
    global pp
    # Flags
    cycle_flag = simulator_config["cycle_flag"]
    objdebug_flag = simulator_config["objdebug_flag"]
    # TODO Objdebug flag
    # Executable
    runtype = simulator_config["runtype"]
    if runtype == "1":
        simulator = simulator_config["simulator_exe_type1"]
    elif runtype == "2":
        simulator = simulator_config["simulator_exe"]
    else:
        logger.critical("Invalid runtype: %s - defaulting to 2" % str(runtype))

    assert(os.path.isfile(simulator))
    # Create a directory
    # TODO Option: have a scratch test directory vs a date today directory
    # Currently have the date today
    # Do the date today method.
    t = datetime.date.today()
    datestr = "%d-%02d%02d" % (t.year, t.month, t.day)
    print "TODAY:", datestr
    work_dir = simulator_config["sim_work_dir"]
    olddir = os.getcwd()
    os.chdir( work_dir )
    work_today = create_work_directory( work_dir, logger = logger )
    os.chdir( work_today )
    # Get the benchmark directories
    specjvm_dir = global_config["specjvm_dir"]
    dacapo_dir = global_config["dacapo_dir"]
    minibench_dir = global_config["minibench_dir"]
    # Trace drop location for simulator output
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # TODO backup_cycle_cpp_dir = global_config["backup_cycle_cpp_dir"]
    # TODO # Backup old simulator output into backup directory
    # TODO num_backed_up = backup_old_simulator_output( cycle_cpp_dir, backup_cycle_cpp_dir )
    # Sub process related stuff
    procdict = {}
    procs = {}
    for bmark in worklist.keys():
        hostlist = worklist[bmark]
        if ( not check_host( benchmark = bmark,
                             hostlist = hostlist,
                             host_config = host_config ) ):
            logger.debug( "SKIP: %s" % bmark )
            continue
        # -----------------------------------------------------------------------
        # Then spawn using multiprocessing
        if is_specjvm(bmark):
            tracefile = specjvm_dir + bmark_config[bmark] 
            namesfile = specjvm_dir + names_config[bmark]
        elif is_dacapo(bmark):
            tracefile = dacapo_dir + bmark_config[bmark]
            namesfile = dacapo_dir + names_config[bmark]
        elif is_minibench(bmark):
            tracefile = minibench_dir + bmark_config[bmark]
            namesfile = minibench_dir + names_config[bmark]
        else:
            print "Benchmark not found: %s" % bmark
            assert(False)
        try:
            assert(os.path.isfile(tracefile))
        except:
            print "%s NOT FOUND." % tracefile
            raise ValueError("%s NOT FOUND." % tracefile)
        try:
            assert(os.path.isfile(namesfile))
        except:
            print "%s NOT FOUND." % namesfile
            raise ValueError("%s NOT FOUND." % namesfile)
        # 
        # Setup the necessary information for running 'simulator'
        #
        basename = bmark + "-cpp-" + str(datestr)
        output_name = os.path.join( cycle_cpp_dir, basename + "-OUTPUT.txt" )
        main_class, main_function = mainfunction_config[bmark].split(".")
        # TODO DEBUG print "%s: %s -> %s" % (bmark, main_class, main_function)
        # ./simulator xalan.names xalan-cpp-2016-0129 CYCLE OBJDEBUG
        myargs = [ namesfile, basename,
                   cycle_flag, objdebug_flag,
                   main_class, main_function ]
        fp = get_trace_fp( tracefile, logger )
        outfptr = open( output_name, "wb" )
        logger.debug( "Tracefile: %s" % tracefile )
        logger.debug( "Output name: %s" % output_name )
        logger.debug( "Working directory: %s" % cycle_cpp_dir )
        timenow = time.asctime()
        cmd = [ simulator ] + myargs
        logger.debug( "Command: [ %s ]" % str(cmd) )
        logger.debug( "[%s] - starting at %s" % (bmark, timenow) )
        p = Process( target = run_subprocess,
                     args = ( cmd,     # simulator command
                              outfptr, # stdout
                              fp,      # stdin
                              outfptr, # sterr
                              cycle_cpp_dir ) ) # change current working directory
        p.start()
        procs[bmark] = p
    # Poll the processes
    done = False
    while not done:
        done = True
        for bmark in procs.keys():
            print ".",
            proc = procs[bmark]
            proc.join(60)
            if proc.is_alive():
                done = False
            else:
                del procs[bmark]
                timenow = time.asctime()
                logger.debug( "[%s] - done at %s" % (bmark, timenow) )
    print "DONE."
    exit(0)
    # HERE: TODO
    # 1. Cyclic garbage vs ref count reclaimed:
    #      * Number of objects
    #      * size of objects
    # 2. Number of cycles
    # 3. Size of cycles
    print "GLOBAL:"
    pp.pprint(global_config)
    results = {}
    count = 0
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
                         help = "Specify global configuration filename.",
                         action = "store" )
    parser.add_argument( "--simconfig",
                         help = "Specify run configuration filename.",
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
    parser.set_defaults( logfile = "run_simulator.log",
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

def process_global_config( args ):
    assert( args.config != None )
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( args.config )
    return { "global" : config_section_map( "global", config_parser ),
             "host" : config_section_map( "hosts", config_parser ),
             "simulator" : config_section_map( "simulator", config_parser ), }

def process_sim_config( args ):
    assert( args.simconfig != None )
    simconfig_parser = ConfigParser.ConfigParser()
    simconfig_parser.read( args.simconfig )
    return { "benchmarks" : config_section_map( "benchmarks", simconfig_parser ),
             "worklist" : config_section_map( "worklist", simconfig_parser ),
             "dacapo" : config_section_map( "dacapo", simconfig_parser ),
             "dacapo_names" : config_section_map( "dacapo_names", simconfig_parser ),
             "specjvm" : config_section_map( "specjvm", simconfig_parser ),
             "specjvm_names" : config_section_map( "specjvm_names", simconfig_parser ),
             "minibench" : config_section_map( "minibench", simconfig_parser ),
             "minibench_names" : config_section_map( "minibench_names", simconfig_parser ),
             "main_function" : config_section_map( "main_function", simconfig_parser ), }

def main():
    global pp

    parser = create_parser()
    args = parser.parse_args()
    benchmark = args.benchmark
    assert( args.config != None )
    assert( args.simconfig != None )
    global_result = process_global_config( args )
    global_config = global_result["global"]
    host_config = process_host_config( global_result["host"] )
    simulator_config = global_result["simulator"]
    sim_result = process_sim_config( args )
    benchmarks = sim_result["benchmarks"]
    worklist = process_worklist_config( sim_result["worklist"] )
    dacapo_config = sim_result["dacapo"]
    dacapo_names = sim_result["dacapo_names"]
    specjvm_config = sim_result["specjvm"]
    specjvm_names = sim_result["specjvm_names"]
    minibench_config = sim_result["minibench"]
    minibench_names = sim_result["minibench_names"]
    mainfunction_config = sim_result["main_function"]

    debugflag = global_config["debug"]
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = debugflag  )
    #
    # Main processing
    #
    return main_process( debugflag = debugflag,
                         worklist = worklist,
                         output = args.output,
                         global_config = global_config,
                         bmark_config = dict( dict(specjvm_config, **dacapo_config), **minibench_config ),
                         names_config = dict( dict(specjvm_names, **dacapo_names), **minibench_names ),
                         simulator_config = simulator_config,
                         host_config = host_config,
                         mainfunction_config = mainfunction_config,
                         logger = logger )

if __name__ == "__main__":
    main()
