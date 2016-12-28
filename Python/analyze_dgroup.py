from __future__ import division
# analyze_dgroup.py
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
import cPickle
# Possible useful libraries, classes and functions:
# from operator import itemgetter
from collections import Counter
from collections import defaultdict
#   - This one is my own library:
from mypytools import mean, stdev, variance
from mypytools import check_host, create_work_directory, process_host_config, \
    process_worklist_config

# TODO: Do we need 'sqorm.py' or 'sqlitetools.py'?
#       Both maybe? TODO

# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import ObjectInfoReader
#     ObjectInfoFile2DB, EdgeInfoFile2DB, StabilityReader

# Needed to read in *-OBJECTINFO.txt and other files from
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "analyze_dgroup.py.log",
                  logger_name = 'analyze_dgroup.py',
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

def check_diedby_stats( dgroups_data = {},
                        objreader = {} ):
    result = defaultdict( dict )
    tmp = 0
    for gnum, glist in dgroups_data["group2list"].iteritems():
        result[gnum]["diedby"] = Counter()
        result[gnum]["actual_ts"] = Counter()
        for objId in glist:
            cause = objreader.get_death_cause(objId)
            last_actual_ts = objreader.get_last_actual_timestamp(objId)
            result[gnum]["diedby"][cause] += 1
            result[gnum]["actual_ts"][last_actual_ts] += 1
        # DEBUG
        tmp += 1
        if tmp >= 20:
            break
    return result

def read_dgroups_from_pickle( result = [],
                              bmark = "",
                              workdir = "",
                              mprflag = False,
                              dgroups2db_config = {},
                              objectinfo_db_config = {},
                              cycle_cpp_dir = "",
                              obj_cachesize = 5000000,
                              debugflag = False,
                              logger = None ):
    assert(logger != None)
    # print os.listdir( )
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
    # Read in DGROUPS from the pickle file
    picklefile = os.path.join( dgroups2db_config["output"],
                               bmark + dgroups2db_config["file-dgroups"] )
    assert(os.path.isfile(picklefile))
    with open(picklefile, "rb") as fptr:
        dgroups_data = cPickle.load(fptr)
    #===========================================================================
    # Process
    # 
    # Idea 1: for each group, check that each object in the group have the same
    # died by
    diedby_results = check_diedby_stats( dgroups_data = dgroups_data,
                                         objreader = objreader )
    print "==========================================================================="
    for gnum, datadict in diedby_results.iteritems():
        assert("diedby" in datadict)
        assert("actual_ts" in datadict)
        print "GROUP %d" % gnum
        print "    * DIEDBY:"
        for diedbytype, total in datadict["diedby"].iteritems():
            print "        %s -> %d" % (diedbytype, total)
        max_tstamp = max( datadict["actual_ts"].keys() )
        min_tstamp = min( datadict["actual_ts"].keys() )
        print "    * MAX actual timestamp: %d" % max_tstamp
        print "        - with # objects  : %d" % datadict["actual_ts"][max_tstamp]
        print "    * min actual timestamp: %d" % min_tstamp
        print "==========================================================================="
    #===========================================================================
    # Idea 2: Get the key objects TODO TODO TODO
    #
    for gnum, glist in dgroups_data["group2list"].iteritems():
        # - for every death group dg:
        for objId in glist:
    #           get the last edge for every object
    #           look for the last edge with the latest death time
    #           save as list as there MAY be more than one last edge
    #       Got the last edge
    #       Save per group
    #       // STATS
    #       * type of key object
    #       * size stats for groups that died by stack
    #            + first should be number of key objects
    #            + then average size of sub death group

    #===========================================================================
    # Write out to ???? TODO
    # 
    # pickle_filename = os.path.join( workdir, bmark + "-DGROUPS.pickle" )
    

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

def get_last_edge_record_for_group( group = None,
                                    edgeinfo = None,
                                    objectinfo = None,
                                    group_dtime = None ):
    latest = 0 # Time of most recent
    srclist = []
    tgt = 0
    # If the group died by stack, then there are no last edges 
    # from the heap to speak of. We look for objects without last edges.
    assert( len(group) > 0 )
    died_by_stack = 
    died_by_heap = objectinfo.died_by_heap(group[0]) )
    if objectinfo.died_by_stack( group[0]) ):
        # We look for all edges that do not have any incoming edge with
        # the same death time as itself. These are the ROOTS.
        for obj in group:
            # TODO: Need a get all edges that target 'obj'
            srclist = edgeinfo.get_sources(obj)
            # TODO TODO TODO: HERE 28 Dec 2016
            # TODO cand = [ x for x in srclist if ]
            # TODO TODO TODO: HERE 28 Dec 2016
    elif objectinfo.died_by_heap( group[0]) ):
        # All edges should have the same death time as the group, except
        # for EXACTLY ONE edge with death time less than group death time.
        elif rec["dtime"] < latest:
            # If there is a last edge which died before the group,
            # then the group shouldn't have died by stack. Furthermore,
            # there can only be one such edge.
            latest = rec["dtime"]
            edgerec = { "srclist" : rec["lastsources"],
                        "tgt" : obj, }
            edgelist = [ edgerec ]
    return { "dtime" : latest,
             "lastsources" : srclist,
             "target" : tgt }

def main_process( global_config = {},
                  main_config = {},
                  worklist_config = {},
                  host_config = {},
                  dgroups2db_config = {},
                  objectinfo_db_config = {},
                  cachesize_config = {},
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
        cachesize = int(cachesize_config[bmark])
        if mprflag:
            assert(False)
            print "=======[ Spawning %s ]================================================" \
                % bmark
            results[bmark] = manager.list([ bmark, ])
            # NOTE: The order of the args tuple is important!
            # ======================================================================
            # Read in the death groups from dgroups2db.py 
            p = Process( target = read_dgroups_from_pickle,
                         args = ( results[bmark],
                                  bmark,
                                  workdir,
                                  mprflag,
                                  dgroups2db_config,
                                  objectinfo_db_config,
                                  cycle_cpp_dir,
                                  cachesize,
                                  debugflag,
                                  logger ) )
            procs_dgroup[bmark] = p
            p.start()
        else:
            print "=======[ Running %s ]=================================================" \
                % bmark
            print "     Reading in death groups..."
            results[bmark] = [ bmark, ]
            read_dgroups_from_pickle( result = results[bmark],
                                      bmark = bmark,
                                      workdir = workdir,
                                      mprflag = mprflag,
                                      dgroups2db_config = dgroups2db_config,
                                      objectinfo_db_config = objectinfo_db_config,
                                      cycle_cpp_dir = cycle_cpp_dir,
                                      obj_cachesize = cachesize,
                                      debugflag = debugflag,
                                      logger = logger )
        break
    print "DONE."
    exit(100)
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
    print "analyze_dgroup.py - DONE."
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
    main_config = config_section_map( "analyze_dgroup", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    # Reuse the dgroups2db-worklist
    worklist_config = config_section_map( "dgroups2db-worklist", config_parser )
    # We take the file output of dgroups2db as input
    dgroups2db_config = config_section_map( "dgroups2db", config_parser )
    objectinfo_db_config = config_section_map( "objectinfo-db", config_parser )
    # Reuse the cachesize
    cachesize_config = config_section_map( "create-supergraph-obj-cachesize", config_parser )
    # MAYBE: edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    # MAYBE: summary_config = config_section_map( "summary_cpp", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "worklist" : worklist_config,
             "hosts" : host_config,
             "dgroups2db" : dgroups2db_config,
             "objectinfo_db" : objectinfo_db_config,
             "cachesize" : cachesize_config
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
    parser.set_defaults( logfile = "analyze_dgroup.log",
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
    dgroups2db_config = configdict["dgroups2db"]
    objectinfo_db_config = configdict["objectinfo_db"]
    cachesize_config = configdict["cachesize"]
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
    pp.pprint( dgroups2db_config )
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
                         dgroups2db_config = dgroups2db_config,
                         objectinfo_db_config = objectinfo_db_config,
                         cachesize_config = cachesize_config,
                         # MAYBE edgeinfo_config = edgeinfo_config,
                         # MAYBE stability_config = stability_config,
                         mprflag = args.mprflag,
                         logger = logger )

if __name__ == "__main__":
    main()
