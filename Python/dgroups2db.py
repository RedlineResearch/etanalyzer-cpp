from __future__ import division
# dgroups2db.py
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
import sqlite3
from shutil import copy 
# Possible useful libraries, classes and functions:
# from operator import itemgetter
# from collections import Counter
# from collections import defaultdict
#   - This one is my own library:
# from mypytools import mean, stdev, variance
from mypytools import check_host, create_work_directory, process_host_config, \
    process_worklist_config

# TODO: Do we need 'sqorm.py' or 'sqlitetools.py'?
#       Both maybe? TODO

# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import ObjectInfoFile2DB, EdgeInfoFile2DB, StabilityReader

# Needed to read in *-OBJECTINFO.txt and other files from
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "dgroups2db.py.log",
                  logger_name = 'dgroups2db.py',
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


#
# Main processing
#

def read_objectinfo_into_db( result = [],
                             bmark = "",
                             outdbname = "",
                             mprflag = False,
                             objectinfo_config = {},
                             cycle_cpp_dir = "",
                             logger = None ):
    assert(logger != None)
    # print os.listdir( )
    tracefile = os.path.join( cycle_cpp_dir, objectinfo_config[bmark] )
    # The ObjectInfoFile2DB will create the DB connection. We just
    # need to pass it the DB filename
    objinforeader = ObjectInfoFile2DB( objinfo_filename = tracefile,
                                       outdbfilename = outdbname,
                                       logger = logger )

def read_edgeinfo_with_stability_into_db( result = [],
                                          bmark = "",
                                          outdbname = "",
                                          mprflag = False,
                                          stabreader = {},
                                          edgeinfo_config = {},
                                          cycle_cpp_dir = "",
                                          logger = None ):
    assert(logger != None)
    print "A:"
    # print os.listdir( )
    tracefile = os.path.join( cycle_cpp_dir, edgeinfo_config[bmark] )
    # The EdgeInfoFile2DB will create the DB connection. We just
    # need to pass it the DB filename
    edgereader = EdgeInfoFile2DB( edgeinfo_filename = tracefile,
                                  outdbfilename = outdbname,
                                  stabreader = stabreader,
                                  logger = logger )
    print "B:"

def main_process( global_config = {},
                  main_config = {},
                  worklist_config = {},
                  host_config = {},
                  objectinfo_config = {},
                  edgeinfo_config = {},
                  stability_config = {},
                  mprflag = False,
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the summary CSV files are
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
    results_obj = {}
    procs_obj = {}
    results_edge = {}
    procs_edge = {}
    dblist = []
    for bmark in worklist_config.keys():
        hostlist = worklist_config[bmark]
        if not check_host( benchmark = bmark,
                           hostlist = hostlist,
                           host_config = host_config ):
            continue
        # Else we can run for 'bmark'
        if mprflag:
            print "=======[ Spawning %s ]================================================" \
                % bmark
            results_obj[bmark] = manager.list([ bmark, ])
            # TODO
            # - create function 'csvinfo2b' that does the work
            #
            # NOTE: The order of the args tuple is important!
            # ======================================================================
            # Read in the OBJECTINFO
            outdbname_object = os.path.join( workdir, bmark + "-OBJECTINFO.db" )
            p = Process( target = read_objectinfo_into_db,
                         args = ( results_obj[bmark],
                                  bmark,
                                  outdbname_object,
                                  mprflag,
                                  objectinfo_config,
                                  cycle_cpp_dir,
                                  logger ) )
            procs_obj[bmark] = p
            dblist.append( outdbname_object )
            p.start()
            # ======================================================================
            # Read in the EDGEINFO
            # TODO TODO TODO
            # Need to read in the StabilityReader
            print "Reading in the STABILITY file for benchmark:", bmark
            sys.stdout.flush()
            stabreader = StabilityReader( os.path.join( cycle_cpp_dir,
                                                        stability_config[bmark] ),
                                          logger = logger )
            stabreader.read_stability_file()
            print "STAB done."
            outdbname_edge = os.path.join( workdir, bmark + "-EDGEINFO.db" )
            results_edge[bmark] = manager.list([ bmark, ])
            # Start the process
            p = Process( target = read_edgeinfo_with_stability_into_db,
                         args = ( results_edge[bmark],
                                  bmark,
                                  outdbname_edge,
                                  mprflag,
                                  stabreader,
                                  edgeinfo_config,
                                  cycle_cpp_dir,
                                  logger )
                         )
            procs_edge[bmark] = p
            dblist.append( outdbname_edge )
            p.start()
        else:
            print "=======[ Running %s ]=================================================" \
                % bmark

            print "     Reading in objectinfo..."
            outdbname_object = os.path.join( workdir, bmark + "-OBJECTINFO.db" )
            results_obj[bmark] = [ bmark, ]
            read_objectinfo_into_db( result = results_obj[bmark],
                                     bmark = bmark,
                                     outdbname = outdbname_object,
                                     mprflag = mprflag,
                                     objectinfo_config = objectinfo_config,
                                     cycle_cpp_dir = cycle_cpp_dir,
                                     logger = logger )
            dblist.append( outdbname_object )
            stabreader = StabilityReader( os.path.join( cycle_cpp_dir,
                                                        stability_config[bmark] ),
                                          logger = logger )
            stabreader.read_stability_file()
            print "STAB done."
            print "     Reading in edgeinfo..."
            outdbname_edge = os.path.join( workdir, bmark + "-EDGEINFO.db" )
            results_edge[bmark] = [ bmark, ]
            read_edgeinfo_with_stability_into_db( result = results_edge[bmark],
                                                  bmark = bmark,
                                                  outdbname = outdbname_edge,
                                                  mprflag = mprflag,
                                                  edgeinfo_config = edgeinfo_config,
                                                  cycle_cpp_dir = cycle_cpp_dir,
                                                  logger = logger )
            dblist.append( outdbname_edge )
    if mprflag:
        # Poll the processes 
        done = False
        while not done:
            done = True
            for bmark in set(procs_obj.keys() + procs_edge.keys()) :
                if bmark in procs_obj:
                    proc = procs_obj[bmark]
                    proc.join(60)
                    if proc.is_alive():
                        done = False
                    else:
                        del procs_obj[bmark]
                        timenow = time.asctime()
                        logger.debug( "[%s] - done at %s" % (bmark, timenow) )
                if bmark in procs_edge:
                    proc = procs_edge[bmark]
                    proc.join(60)
                    if proc.is_alive():
                        done = False
                    else:
                        del procs_edge[bmark]
                        timenow = time.asctime()
                        logger.debug( "[%s] - done at %s" % (bmark, timenow) )
        print "======[ Processes DONE ]========================================================"
        sys.stdout.flush()
    print "================================================================================"
    # Copy all the databases into MAIN directory.
    dest = main_config["output"]
    for dbfilename in dblist:
        # Check to see first if the destination exists:
        # print "XXX: %s -> %s" % (dbfilename, dbfilename.split())
        abspath, fname = os.path.split(dbfilename)
        tgt = os.path.join( dest, fname )
        if os.path.isfile(tgt):
            try:
                os.remove(tgt)
            except:
                logger.error( "Weird error: found the file [%s] but can't remove it." % tgt )
        print "Copying %s -> %s." % (dbfilename, dest)
        copy( dbfilename, dest )
    print "================================================================================"
    print "dgroups2db.py - DONE."
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
    main_config = config_section_map( "dgroups2db", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    worklist_config = config_section_map( "dgroups2db-worklist", config_parser )
    dgroups_config = config_section_map( "etanalyze-output", config_parser )
    # TODO objectinfo_config = config_section_map( "objectinfo", config_parser )
    # TODO edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    # TODO stability_config = config_section_map( "stability-summary", config_parser )
    # MAYBE: summary_config = config_section_map( "summary_cpp", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "worklist" : worklist_config,
             "hosts" : host_config,
             "dgroups" : dgroups_config,
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
    parser.set_defaults( logfile = "dgroups2db.log",
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
    main_config = configdict["main"]
    worklist_config = process_worklist_config( configdict["worklist"] )
    host_config = process_host_config( configdict["hosts"] )
    dgroups_config = configdict["dgroups"]
    # TODO objectinfo_config = configdict["objectinfo"]
    # TODO edgeinfo_config = configdict["edgeinfo"]
    # TODO stability_config = configdict["stability"]
    # TODO DEBUG TODO
    print "================================================================================"
    pp.pprint( global_config )
    print "================================================================================"
    pp.pprint( main_config )
    print "================================================================================"
    pp.pprint( host_config )
    print "================================================================================"
    pp.pprint( dgroups_config )
    # TODO END DEBUG TODO
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    exit(100)
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         global_config = global_config,
                         main_config = main_config,
                         host_config = host_config,
                         worklist_config = worklist_config,
                         objectinfo_config = objectinfo_config,
                         edgeinfo_config = edgeinfo_config,
                         stability_config = stability_config,
                         mprflag = args.mprflag,
                         logger = logger )

if __name__ == "__main__":
    main()