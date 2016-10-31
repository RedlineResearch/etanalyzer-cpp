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
from garbology import DeathGroupsReader, ObjectInfoReader
#     ObjectInfoFile2DB, EdgeInfoFile2DB, StabilityReader

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

def read_dgroups_into_pickle( result = [],
                              bmark = "",
                              workdir = "",
                              mprflag = False,
                              dgroups_config = {},
                              cycle_cpp_dir = "",
                              objectinfo_db_config = {},
                              obj_cachesize = 5000000,
                              debugflag = False,
                              logger = None ):
    assert(logger != None)
    # print os.listdir( )
    tracefile = os.path.join( cycle_cpp_dir, dgroups_config[bmark] )
    #===========================================================================
    # Read in OBJECTINFO
    print "Reading in the OBJECTINFO file for benchmark:", bmark
    sys.stdout.flush()
    oread_start = time.clock()
    print " - Using objectinfo DB:"
    db_filename = os.path.join( cycle_cpp_dir,
                                objectinfo_db_config[bmark] )
    objreader = ObjectInfoReader( useDB_as_source = True,
                                  db_filename = db_filename,
                                  cachesize = obj_cachesize,
                                  logger = logger )
    objreader.read_objinfo_file()
    #===========================================================================
    # Read in DGROUPS
    dgroups_reader = DeathGroupsReader( dgroup_file = tracefile,
                                        clean_flag = True,
                                        debugflag = debugflag,
                                        logger = logger )
    dgroups_reader.read_dgroup_file( objreader )
    #===========================================================================
    # Write out to pickle and csv files
    # 
    pickle_filename = os.path.join( workdir, bmark + "-DGROUPS.pickle" )
    group2list_filename = os.path.join( workdir, bmark + "-DGROUPS-group2list.csv" )
    obj2group_filename = os.path.join( workdir, bmark + "-DGROUPS-obj2group.csv" )
    dgroups_reader.write_clean_dgroups_to_file( # outdbname,
                                                pickle_filename = pickle_filename,
                                                group2list_filename = group2list_filename,
                                                obj2group_filename = obj2group_filename,
                                                object_info_reader = objreader )

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
                  dgroups_config = {},
                  objectinfo_db_config = {},
                  # TODO objectinfo_config = {},
                  # TODO edgeinfo_config = {},
                  # TODO stability_config = {},
                  mprflag = False,
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the summary CSV files are. We get the
    # bmark-CYCLES.csv files here.
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
    procs_dgroup = {}
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
            results[bmark] = manager.list([ bmark, ])
            # NOTE: The order of the args tuple is important!
            # ======================================================================
            # Read in the CYCLES (death groups file from simulator) 
            p = Process( target = read_dgroups_into_pickle,
                         args = ( results[bmark],
                                  bmark,
                                  workdir,
                                  mprflag,
                                  dgroups_config,
                                  cycle_cpp_dir,
                                  objectinfo_db_config,
                                  debugflag,
                                  logger ) )
            procs_dgroup[bmark] = p
            p.start()
        else:
            print "=======[ Running %s ]=================================================" \
                % bmark
            print "     Reading in cycles (death groups)..."
            results[bmark] = [ bmark, ]
            read_dgroups_into_pickle( result = results[bmark],
                                      bmark = bmark,
                                      workdir = workdir,
                                      mprflag = mprflag,
                                      dgroups_config = dgroups_config,
                                      cycle_cpp_dir = cycle_cpp_dir,
                                      objectinfo_db_config = objectinfo_db_config,
                                      debugflag = debugflag,
                                      logger = logger )
            dblist.append( outdbname )
    if mprflag:
        # Poll the processes 
        done = False
        while not done:
            done = True
            for bmark in procs_dgroup.keys():
                proc = procs_dgroup[bmark]
                proc.join(60)
                if proc.is_alive():
                    done = False
                else:
                    del procs_dgroup[bmark]
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
        # Split the absolute filename into a path and file pair:
        abspath, fname = os.path.split(dbfilename)
        # Use the same filename added to the destination path
        tgt = os.path.join( dest, fname )
        if os.path.isfile(tgt):
            try:
                os.remove(tgt)
            except:
                logger.error( "Weird error: found the file [%s] but can't remove it. The copy might fail." % tgt )
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
    objectinfo_db_config = config_section_map( "objectinfo-db", config_parser )
    # TODO objectinfo_config = config_section_map( "objectinfo", config_parser )
    # TODO edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    # TODO stability_config = config_section_map( "stability-summary", config_parser )
    # MAYBE: summary_config = config_section_map( "summary_cpp", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "worklist" : worklist_config,
             "hosts" : host_config,
             "dgroups" : dgroups_config,
             "objectinfo_db" : objectinfo_db_config,
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
    objectinfo_db_config = configdict["objectinfo_db"]
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
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         global_config = global_config,
                         main_config = main_config,
                         host_config = host_config,
                         worklist_config = worklist_config,
                         dgroups_config = dgroups_config,
                         objectinfo_db_config = objectinfo_db_config,
                         # TODO objectinfo_config = objectinfo_config,
                         # TODO edgeinfo_config = edgeinfo_config,
                         # TODO stability_config = stability_config,
                         mprflag = args.mprflag,
                         logger = logger )

if __name__ == "__main__":
    main()
