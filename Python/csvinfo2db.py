from __future__ import division
# csvinfo2db.py.py
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
from garbology import ObjectInfoFile2DB

# Needed to read in *-OBJECTINFO.txt and other files from
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "csvinfo2db.py.log",
                  logger_name = 'csvinfo2db.py',
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
    # Create the DB file.
    # Find the files.
    tracefile = os.path.join( cycle_cpp_dir, objectinfo_config[bmark] )
    # The ObjectInfoFile2DB will create the DB connection. We just
    # need to pass it the DB filename
    objinforeader = ObjectInfoFile2DB( objinfo_filename = tracefile,
                                       outdbfilename = outdbname,
                                       logger = logger )

def read_edgeinfo_into_db( result = [],
                           mprflag = False,
                           logger = None ):
    assert(logger != None)
    # Find the files.

def main_process( output = None,
                  global_config = {},
                  main_config = {},
                  worklist_config = {},
                  host_config = {},
                  objectinfo_config = {},
                  mprflag = False,
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the summary CSV files are
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Setup stdout to file redirect TODO: Where should this comment be placed?
    # TODO: Eventually remove the following commented code related to hosts.
    # Since we're not doing mutiprocessing, we don't need this. But keep
    # it here until absolutely sure.
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
    # TODO: Need a worklist.
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    manager = Manager()
    results = {}
    for bmark in worklist_config.keys():
        hostlist = worklist_config[bmark]
        if not check_host( benchmark = bmark,
                           hostlist = hostlist,
                           host_config = host_config ):
            continue
        # Else we can run for 'bmark'
        outdbname = os.path.join( workdir, bmark + "-OBJECTINFO.db" )
        if mprflag:
            print "=======[ Spawning %s ]================================================" \
                % bmark
            results[bmark] = manager.list([ bmark, ])
            # TODO
            # - create function 'csvinfo2b' that does the work
            #
            # NOTE: The order of the args tuple is important!
            # Read in the OBJECTINFO
            read_objectinfo_into_db( result = results[bmark],
                                     bmark = bmark,
                                     outdbname = outdbname,
                                     mprflag = mprflag,
                                     objectinfo_config = objectinfo_config,
                                     cycle_cpp_dir = cycle_cpp_dir,
                                     logger = logger )
            # TODO p = Process( target = read_objectinfo_into_db,
            # TODO              args = ( results[bmark],
            # TODO                       bmark,
            # TODO                       outdbname,
            # TODO                       mprflag,
            # TODO                       objectinfo_config,
            # TODO                       cycle_cpp_dir,
            # TODO                       logger ) )
            # TODO procs[bmark] = p
            # TODO p.start()
            # Read in the EDGEINFO
            # TODO p = Process( target = read_edgeinfo_into_db,
            # TODO              args = ( results[bmark],
            # TODO                       mprflag,
            # TODO                       logger ) )
            # TODO procs[bmark] = p
            # TODO p.start()
        else:
            assert(False)
            results[bmark] = [ bmark, ]
            create_supergraph_all_MPR( bmark = bmark,
                                       cycle_cpp_dir = cycle_cpp_dir,
                                       main_config = main_config,
                                       objectinfo_config = objectinfo_config,
                                       dgroup_config = dgroup_config,
                                       stability_config = stability_config,
                                       reference_config = reference_config,
                                       reverse_ref_config = reverse_ref_config,
                                       summary_config = summary_config,
                                       fmain_result = fmain_result,
                                       result = results[bmark],
                                       logger = logger )

    exit(100)
    print "================================================================================"
    exit(1)
    with open( os.path.join( workdir, "TODO.csv" ), "wb" ) as fptr:
        pass
        # writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
        # for row in table1:
        #     writer.writerow(row)
    print "csvinfo2db.py.py - DONE."
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
    main_config = config_section_map( "csvinfo2db", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    worklist_config = config_section_map( "cvsinfo2db-worklist", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    # MAYBE: summary_config = config_section_map( "summary_cpp", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "worklist" : worklist_config,
             "hosts" : host_config,
             "objectinfo" : objectinfo_config,
             }

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
    parser.set_defaults( logfile = "csvinfo2db.py.log",
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
    objectinfo_config = configdict["objectinfo"]
    # TODO DEBUG TODO
    pp.pprint( global_config )
    print "================================================================================"
    pp.pprint( main_config )
    print "================================================================================"
    pp.pprint( host_config )
    # TODO END DEBUG TODO
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         global_config = global_config,
                         main_config = main_config,
                         host_config = host_config,
                         worklist_config = worklist_config,
                         objectinfo_config = objectinfo_config,
                         mprflag = args.mprflag,
                         logger = logger )

if __name__ == "__main__":
    main()
