from __future__ import division
# run_GCsim.py
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
from multiprocessing import Process, Manager
from shutil import copy 
import subprocess
# import sqlite3
# Possible useful libraries, classes and functions:
# from operator import itemgetter
# from collections import Counter
# from collections import defaultdict
#   - This one is my own library:
# from mypytools import mean, stdev, variance
from mypytools import check_host, create_work_directory, process_host_config, \
    process_worklist_config, is_specjvm, is_dacapo, is_minibench, get_trace_fp

# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import SummaryReader
#     DeathGroupsReader, ObjectInfoReader
#     ObjectInfoFile2DB, EdgeInfoFile2DB, StabilityReader

import csv

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "run_GCsim.py.log",
                  logger_name = 'run_GCsim.py',
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

def get_trace_and_name_file( bmark = None,
                             bmark_config = {},
                             names_config = {},
                             specjvm_dir = None,
                             dacapo_dir = None,
                             minibench_dir = None ):
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
    return { "trace" : tracefile,
             "names" : namesfile, }

def run_GC_simulator( result = {},
                      simulator = None,
                      bmark = None,
                      workdir = None,
                      mprflag = False,
                      main_config = {},
                      dgroups2db_config = {},
                      tracefile = {},
                      namesfile = {},
                      max_live_size = None,
                      debugflag = False,
                      numprocs = 1,
                      logger = None ):
    # 
    #  To get the heap size, we start at the initial heap size given in main_config.
    #  We iterate until we get to maximum process per benchmark OR we reach the maximum.
    #  If the maximum given is 0, then we go on until no GC is needed.
    #  
    #  Q: How do we name the files?
    # 
    # Setup the necessary information for running 'simulator'
    #
    # Get the location for the *-DGROUPS-group2list.csv files
    group2list_dir = dgroups2db_config["output"]
    assert( os .path.isdir(group2list_dir) )
    # -    because the output dir of the script is where we can find the output.
    template = dgroups2db_config["file-group2list"]
    group2list_filename = os.path.join( group2list_dir, bmark + template )
    heapsize = ((int(max_live_size * 1.05) + 8) // 8) * 8
    start_heapsize = heapsize
    count = 0
    procs = {}
    while True:
        index = len(procs)
        while ( (index < numprocs) and
                (heapsize <= (start_heapsize * 4)) ):
            index += 1
            count += 1
            # Output file
            print "================================================================================"
            print "  STARTING %s - %d" % (bmark, count)
            output_file = os.path.join( workdir, bmark + "-simulator-GC-%d.txt" % count )
            logger.debug( "Tracefile: %s" % tracefile )
            logger.debug( "Output name: %s" % output_file )
            with open(output_file, "wb") as out_fileptr:
                # Command looks like this:
                #     cat tracefile | simulator-GC lusearch.names lusearch-DGROUPS-group2list.csv lusearch 5000000
                myargs = [ namesfile, group2list_filename, bmark, str(heapsize), "DEF", ]
                stdout_filename = bmark + main_config["file-output-template"]
                cmd = [ simulator ] + myargs
                print "CMD:", cmd
                logger.debug( "Process[ %d ]: command[ %s ]" % (count, str(cmd)) )
                # TODO logger.debug( "Process[%d: %s] - starting at %s" % (bmark, timenow) )
                # TODO: implement this so we can use it in a 'with ... as' statement
                tracefp = get_trace_fp( tracefile, logger )
                sproc = subprocess.Popen( cmd,
                                          stdout = out_fileptr,
                                          stdin = tracefp,
                                          stderr = out_fileptr,
                                          cwd = workdir )
                # Spawn all at once and communicate at the end
                procs[count] = sproc
            heapsize = heapsize + (((int(max_live_size * 0.5) + 8) // 8) * 8)
        check_done = False
        check_count = 0
        while not check_done:
            for procnum in procs.keys():
                proc = procs[procnum]
                proc.poll()
                if proc.returncode != None:
                    del procs[procnum]
                    timenow = time.asctime()
                    logger.debug( "[%s : %d] - done at %s" % (bmark, procnum, timenow) )
                    print ">>> [%s : %d] - done at %s" % (bmark, procnum, timenow)
                    check_done = True
                    break
                else:
                    check_count += 1
            time.sleep(10)
        logger.debug( ">>> NUM PROCS[ %d ]" % len(procs) )
        if ( (len(procs) == 0) and
             (heapsize >= (start_heapsize * 4)) ):
            break
    print "DEBUG."
    exit(100)

#
# Main processing
#

def main_process( global_config = {},
                  main_config = {},
                  worklist_config = {},
                  host_config = {},
                  bmark_config = {},
                  names_config = {},
                  dgroups2db_config = {},
                  simulator_config = {},
                  summary_config = {},
                  mprflag = False,
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the summary CSV files are. We get the
    # bmark-SUMMARY.csv files here.
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
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
    # Timestamped work directories are not deleted unless low
    # in space. This is to be able to go back to a known good dataset.
    # The current run is then copied to a non-timestamped directory
    # where the rest of the workflow expects it as detailed in the config file.
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    manager = Manager()
    results = {}
    procs = {}
    # Get the simulator executable
    simulator = simulator_config["simulatorgc"]
    assert( os.path.isfile(simulator) )
    for bmark in worklist_config.keys():
        hostlist = worklist_config[bmark]
        if not check_host( benchmark = bmark,
                           hostlist = hostlist,
                           host_config = host_config ):
            continue
        # Else we can run for 'bmark'
        tmpresult = get_trace_and_name_file( bmark = bmark,
                                             bmark_config = bmark_config,
                                             names_config = names_config,
                                             specjvm_dir = global_config["specjvm_dir"],
                                             dacapo_dir = global_config["dacapo_dir"],
                                             minibench_dir = global_config["minibench_dir"] )
        #===========================================================================
        # Read in SUMMARY
        print "Reading in the SUMMARY file for benchmark:", bmark
        summary_fname = os.path.join( cycle_cpp_dir,
                                      summary_config[bmark] )
        summary_reader = SummaryReader( summary_file = summary_fname,
                                        logger = logger )
        summary_reader.read_summary_file()
        #===========================================================================
        # Number of processes
        pp.pprint(main_config)
        number_procs_per_benchmark = int(main_config["number-procs-per-benchmark"])
        max_live_size = summary_reader.get_max_live_size()
        if mprflag:
            print "=======[ Spawning %s ]================================================" \
                % bmark
            results[bmark] = manager.list([ bmark, ])
            # NOTE: The order of the args tuple is important!
            # ======================================================================
            # Read in the CYCLES (death groups file from simulator) 
            p = Process( target = run_GC_simulator,
                         args = ( results[bmark],
                                  simulator,
                                  bmark,
                                  workdir,
                                  mprflag,
                                  main_config,
                                  dgroups2db_config,
                                  tmpresult["trace"],
                                  tmpresult["names"],
                                  max_live_size,
                                  debugflag,
                                  number_procs_per_benchmark,
                                  logger ) )
            procs[bmark] = p
            p.start()
        else:
            print "=======[ Running %s ]=================================================" \
                % bmark
            print "     Reading in cycles (death groups)..."
            results[bmark] = [ bmark, ]
            run_GC_simulator( result = results[bmark],
                              simulator = simulator,
                              bmark = bmark,
                              workdir = workdir,
                              mprflag = mprflag,
                              main_config = main_config,
                              dgroups2db_config = dgroups2db_config,
                              tracefile = tmpresult["trace"],
                              namesfile = tmpresult["names"],
                              max_live_size = max_live_size,
                              debugflag = debugflag,
                              numprocs = number_procs_per_benchmark,
                              logger = logger )
    if mprflag:
        # Poll the processes 
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
                    timenow = time.asctime()
                    logger.debug( "[%s] - done at %s" % (bmark, timenow) )
        print "======[ Processes DONE ]========================================================"
        sys.stdout.flush()
    print "================================================================================"
    # Copy all the databases into MAIN directory.
    dest = main_config["output"]
    for filename in os.listdir( workdir ):
        # Check to see first if the destination exists:
        # print "XXX: %s -> %s" % (filename, filename.split())
        # Split the absolute filename into a path and file pair:
        # Use the same filename added to the destination path
        tgtfile = os.path.join( dest, filename )
        if os.path.isfile(tgtfile):
            try:
                os.remove(tgtfile)
            except:
                logger.error( "Weird error: found the file [%s] but can't remove it. The copy might fail." % tgtfile )
        print "Copying %s -> %s." % (filename, dest)
        copy( filename, dest )
    print "================================================================================"
    print "run_GCsim.py - DONE."
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
    main_config = config_section_map( "run-GCsim", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    worklist_config = config_section_map( "run-GCsim-worklist", config_parser )
    dgroups2db_config = config_section_map( "dgroups2db", config_parser )
    simulator_config = config_section_map( "simulator", config_parser )
    summary_config = config_section_map( "summary-cpp", config_parser )
    # TODO objectinfo_config = config_section_map( "objectinfo", config_parser )
    # TODO edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    # TODO stability_config = config_section_map( "stability-summary", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "worklist" : worklist_config,
             "hosts" : host_config,
             "dgroups2db" : dgroups2db_config,
             "simulator" : simulator_config,
             "summary" : summary_config,
             # TODO "objectinfo_db" : objectinfo_db_config,
             # TODO "objectinfo" : objectinfo_config,
             # TODO "edgeinfo" : edgeinfo_config,
             # TODO "stability" : stability_config,
             }

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "--config",
                         help = "Specify configuration filename.",
                         action = "store" )
    parser.add_argument( "--simconfig",
                         help = "Specify simulator configuration filename.",
                         action = "store" )
    parser.add_argument( "--debug",
                         dest = "debugflag",
                         help = "Enable debug output.",
                         action = "store_true" )
    parser.add_argument( "--no-debug",
                         dest = "debugflag",
                         help = "Disable debug output.",
                         action = "store_false" )
    parser.add_argument( "--mpr",
                         dest = "mprflag",
                         help = "Enable multiprocessing.",
                         action = "store_true" )
    parser.add_argument( "--single",
                         dest = "mprflag",
                         help = "Single threaded operation.",
                         action = "store_false" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "run_GCsim.log",
                         debugflag = False,
                         config = None )
    return parser

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
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    assert( args.config != None )
    # Get the configurations we need.
    configdict = process_config( args )
    global_config = configdict["global"]
    main_config = configdict["main"]
    worklist_config = process_worklist_config( configdict["worklist"] )
    host_config = process_host_config( configdict["hosts"] )
    dgroups2db_config = configdict["dgroups2db"]
    simulator_config = configdict["simulator"]
    summary_config = configdict["summary"]
    # Benchmark configurations
    sim_result = process_sim_config( args )
    dacapo_config = sim_result["dacapo"]
    dacapo_names = sim_result["dacapo_names"]
    specjvm_config = sim_result["specjvm"]
    specjvm_names = sim_result["specjvm_names"]
    minibench_config = sim_result["minibench"]
    minibench_names = sim_result["minibench_names"]
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( global_config = global_config,
                         main_config = main_config,
                         host_config = host_config,
                         bmark_config = dict( dict(specjvm_config, **dacapo_config), **minibench_config ),
                         names_config = dict( dict(specjvm_names, **dacapo_names), **minibench_names ),
                         worklist_config = worklist_config,
                         dgroups2db_config = dgroups2db_config,
                         simulator_config = simulator_config,
                         summary_config = summary_config,
                         mprflag = args.mprflag,
                         debugflag = global_config["debug"],
                         logger = logger )

if __name__ == "__main__":
    main()
