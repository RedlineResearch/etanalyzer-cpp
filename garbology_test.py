from __future__ import division
# garbology_test.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import ConfigParser
from collections import Counter, defaultdict
from multiprocessing import Process, Manager
from operator import itemgetter
# TODO import shutil
# TODO import re

# Possible useful libraries, classes and functions:
#   - This one is my own library:
from mypytools import mean, stdev, variance, check_host, \
                      is_specjvm, is_dacapo, is_minibench


# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import ObjectInfoReader, StabilityReader, ReferenceReader, \
         SummaryReader, NamesReader, get_index, is_stable
         
# Needed to read in *-OBJECTINFO.txt and other files from 
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time

# Global pretty printer
pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "garbology_test.log",
                  logger_name = 'garbology_test',
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

def was_allocated_before_main( objId = 0,
                               main_time = 0,
                               objreader = {}):
    atime = objreader.get_alloc_time( objId )
    return atime < main_time

# TODO: Refactor out
def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None

def get_objects_from_stable_group( sgnum = 0,
                                   stable_grouplist = [] ):
    return stable_grouplist[sgnum].nodes() if (sgnum < len(stable_grouplist)) else []

def get_objects_from_stable_group_as_set( sgnum = 0, # stable group number
                                          stable_grouplist = [] ):
    objset = set( get_objects_from_stable_group( sgnum = sgnum,
                                                 stable_grouplist = stable_grouplist ) )
    return objset
    
def is_array( mytype ):
    return (len(mytype) > 0) and (mytype[0] == "[")

#================================================================================
#================================================================================

def read_simulator_data( bmark = "",
                         mydict = {},
                         cycle_cpp_dir = "",
                         objectinfo_config = {},
                         summary_config = {},
                         use_objinfo_db = False,
                         obj_cachesize = 5000000,
                         objectinfo_db_config = {},
                         global_config = {},
                         logger = None ):
    summary_fname = os.path.join( cycle_cpp_dir,
                                  summary_config[bmark] )
    #===========================================================================
    # Read in OBJECTINFO
    print "Reading in the OBJECTINFO file for benchmark:", bmark
    sys.stdout.flush()
    oread_start = time.clock()
    if use_objinfo_db:
        print " - Using objectinfo DB:"
        db_filename = os.path.join( cycle_cpp_dir,
                                    objectinfo_db_config[bmark] )
        oread_start = time.clock()
        mydict["objreader"] = ObjectInfoReader( useDB_as_source = True,
                                                db_filename = db_filename,
                                                cachesize = obj_cachesize,
                                                logger = logger )
        objreader = mydict["objreader"]
        objreader.read_objinfo_file()
    else:
        print " - Using objectinfo text file:"
        objinfo_path = os.path.join( cycle_cpp_dir,
                                     objectinfo_config[bmark] )
        mydict["objreader"] = ObjectInfoReader( objinfo_path,
                                                useDB_as_source = False,
                                                logger = logger )
        objreader = mydict["objreader"]
        objreader.read_objinfo_file()
    oread_end = time.clock()
    logger.debug( "[%s]: DONE: %f" % (bmark, (oread_end - oread_start)) )
    sys.stdout.write(  "[%s]: DONE: %f\n" % (bmark, (oread_end - oread_start)) )
    sys.stdout.flush()
    # #===========================================================================
    # # Read in STABILITY
    # TODO print "Reading in the STABILITY file for benchmark:", bmark
    # TODO sys.stdout.flush()
    # TODO stab_start = time.clock()
    # TODO mydict["stability"] = StabilityReader( os.path.join( cycle_cpp_dir,
    # TODO                                                      stability_config[bmark] ),
    # TODO                                        logger = logger )
    # TODO stabreader = mydict["stability"]
    # TODO stabreader.read_stability_file()
    # TODO stab_end = time.clock()
    # TODO logger.debug( "[%s]: DONE: %f" % (bmark, (stab_end - stab_start)) )
    # TODO sys.stdout.write(  "[%s]: DONE: %f\n" % (bmark, (stab_end - stab_start)) )
    # TODO sys.stdout.flush()
    #===========================================================================
    # Read in SUMMARY
    print "Reading in the SUMMARY file for benchmark:", bmark
    sys.stdout.flush()
    summary_start = time.clock()
    summary_fname = os.path.join( cycle_cpp_dir,
                                  summary_config[bmark] )
    mydict["summary_reader"] = SummaryReader( summary_file = summary_fname,
                                              logger = logger )
    summary_reader = mydict["summary_reader"]
    summary_reader.read_summary_file()
    summary_end = time.clock()
    logger.debug( "[%s]: DONE: %f" % (bmark, (summary_end - summary_start)) )
    sys.stdout.write(  "[%s]: DONE: %f\n" % (bmark, (summary_end - summary_start)) )
    sys.stdout.flush()
    #===========================================================================
    return True


def garbology_test( bmark = "",
                    cycle_cpp_dir = "",
                    main_config = {},
                    objectinfo_config = {},
                    reverse_ref_config = {},
                    summary_config = {},
                    result = [],
                    use_objinfo_db = False,
                    objectinfo_db_config = {},
                    obj_cachesize_config = {},
                    global_config = {},
                    logger = None ):
    # Assumes that we are in the desired working directory.
    # Get all the objects and add as a node to the graph
    mydict = {}
    backupdir = main_config["backup"]
    outputdir = main_config["output"]
    obj_cachesize = int(obj_cachesize_config[bmark])
    # Read all the data in.
    read_result = read_simulator_data( bmark = bmark,
                                       mydict = mydict,
                                       cycle_cpp_dir = cycle_cpp_dir,
                                       objectinfo_config = objectinfo_config,
                                       summary_config = summary_config,
                                       use_objinfo_db = use_objinfo_db,
                                       obj_cachesize = obj_cachesize,
                                       objectinfo_db_config = objectinfo_db_config,
                                       global_config = global_config,
                                       logger = logger )
    if read_result == False:
        return False
    # Extract the important reader objects
    objreader = mydict["objreader"]
    summary_reader = mydict["summary_reader"]
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Summarize stability
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    count = 0
    result = defaultdict( lambda: defaultdict(Counter) )
    counter = Counter()
    for objId, rec in objreader.iteritems():
        count += 1
        dtime = objreader.get_death_time_using_record(rec)
        # assert( dtime > 0 )
        counter[dtime] += 1
    for dtime, count in sorted( counter.items(), key = itemgetter(1), reverse = True ):
        print "%d -> %d" % (dtime, count)
    exit(0)
    # TODO: If we need a CSV output, then I can modify the copy/pasta code following.
    # TODO: Following is dead code
    # TODO: output_filename = os.path.join( outputdir, "%s-STABILITY-SUMMARY.csv" % bmark)
    # TODO: with open(output_filename, "wb") as fptr:
    # TODO:     seen_objects = set()
    # TODO:     # Create the CSV writer and write the header row
    # TODO:     writer = csv.writer( fptr, quoting = csv.QUOTE_MINIMAL )
    # TODO:     writer.writerow( [ "type", "field_id", "field_name", "field_target_type",
    # TODO:                        "stable", "serial-stable", "unstable",
    # TODO:                        "%stable", "%serial-stable", "%unstable", ] )
    # TODO:     for mytype, classdict in result.iteritems():
    # TODO:         # Ignore arrays for now.
    # TODO:         if is_array( mytype ):
    # TODO:             continue
    # TODO:         for fieldId, stabdict in classdict.iteritems():
    # TODO:             row = [ mytype ]
    # TODO:             stabsum = { "S" : 0,
    # TODO:                         "ST" : 0,
    # TODO:                         "U" : 0, }
    # TODO:             for st, cnt in stabdict.iteritems():
    # TODO:                 # TODO print "      %s = %d" % (st, cnt)
    # TODO:                 if st == "S" or st == "ST":
    # TODO:                     stabsum[st] += cnt
    # TODO:                 elif st == "U" or st == "X":
    # TODO:                     stabsum["U"] += cnt
    # TODO:             total = stabsum["S"] + stabsum["ST"] + stabsum["U"]
    # TODO:             field_name = namesreader.get_field_name(fieldId)
    # TODO:             field_target_type = namesreader.get_field_target_type(fieldId)
    # TODO:             row.extend( [ fieldId, field_name, field_target_type,
    # TODO:                           stabsum["S"], stabsum["ST"], stabsum["U"],
    # TODO:                           "{:.4f}".format(stabsum["S"] / total),
    # TODO:                           "{:.4f}".format(stabsum["ST"] / total),
    # TODO:                           "{:.4f}".format(stabsum["U"] / total), ] )
    # TODO:             writer.writerow( row )


def main_process( global_config = {},
                  objectinfo_config = {},
                  host_config = {},
                  worklist_config = {},
                  main_config = {},
                  reference_config = {},
                  summary_config = {},
                  objectinfo_db_config = {},
                  obj_cachesize_config = {},
                  mprflag = False,
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the simulator output files are
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Get the date and time to label the work directory.
    today = date.today()
    today = today.strftime("%Y-%m%d")
    timenow = datetime.now().time().strftime("%H-%M-%S")
    olddir = os.getcwd()
    workdir =  main_config["output"]
    os.chdir( workdir )
    # Where to get file?
    # Filenames are in
    #   - objectinfo_config, reference_config, reverse_ref_config
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    manager = Manager()
    procs = {}
    results = {}
    use_objinfo_db = (main_config["use_objinfo_db"] == "True")
    # TODO
    for bmark in worklist_config.keys():
        hostlist = worklist_config[bmark]
        if not check_host( benchmark = bmark,
                           hostlist = hostlist,
                           host_config = host_config ):
            continue
        if mprflag:
            assert(False) # TODO: Use the single threaded version for now TODO
            # Multiprocessing version
            print "=======[ Spawning %s ]================================================" \
                % bmark
            results[bmark] = manager.list([ bmark, ])
            p = Process( target = garbology_test,
                         args = ( bmark,
                                  cycle_cpp_dir,
                                  main_config,
                                  objectinfo_config,
                                  dgroup_pickle_config,
                                  reference_config,
                                  reverse_ref_config,
                                  summary_config,
                                  fmain_result,
                                  results[bmark],
                                  use_objinfo_db,
                                  use_edgeinfo_db,
                                  objectinfo_db_config,
                                  obj_cachesize_config,
                                  global_config,
                                  logger ) )
            procs[bmark] = p
            p.start()
        else:
            # Single threaded version
            print "=======[ Running %s ]=================================================" \
                % bmark
            results[bmark] = [ bmark, ]
            garbology_test( bmark = bmark,
                            cycle_cpp_dir = cycle_cpp_dir,
                            main_config = main_config,
                            objectinfo_config = objectinfo_config,
                            summary_config = summary_config,
                            result = results[bmark],
                            use_objinfo_db = use_objinfo_db,
                            objectinfo_db_config = objectinfo_db_config,
                            obj_cachesize_config = obj_cachesize_config,
                            global_config = global_config,
                            logger = logger )

    exit(100)
    # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
    # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
    if mprflag:
        # Poll the processes 
        done = False
        expected = len(procs.keys())
        numdone = 0
        # NOTE: We could have checked to see if this is a child process and just
        # skipped this if not the parent. But since the procs dictionary will 
        # be empty for children, this is fine as is.
        while not done:
            done = True
            for bmark in procs.keys():
                proc = procs[bmark]
                proc.join(10)
                if proc.is_alive():
                    done = False
                else:
                    numdone += 1
                    print "==============================> [%s] DONE." % bmark
                    del procs[bmark]
                sys.stdout.flush()
        print "======[ Processes DONE ]========================================================"
    print "================================================================================"
    print "garbology_test.py - DONE."
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
    main_config = config_section_map( "summarize-stability", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    worklist_config = config_section_map( "garbology-test-worklist", config_parser )
    obj_cachesize_config = config_section_map( "summarize-stability-obj-cachesize", config_parser )
    summary_config = config_section_map( "summary-cpp", config_parser )
    objectinfo_db_config = config_section_map( "objectinfo-db", config_parser )
    # MAYBE TODO reference_config = config_section_map( "reference", config_parser )
    # TODO stability_config = config_section_map( "stability-summary", config_parser )
    # MAYBE TODO reverse_ref_config = config_section_map( "reverse-reference", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # TODO edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    # TODO dgroup_config = config_section_map( "etanalyze-output", config_parser )
    # TODO dgroup_pickle_config = config_section_map( "clean-dgroup-pickle", config_parser )
    # TODO edge_cachesize_config = config_section_map( "summarize-stability-edge-cachesize", config_parser )
    # TODO dgroups2db_config = config_section_map( "dgroups2db", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "hosts" : host_config,
             "garbology-test-worklist" : worklist_config,
             "summary_config" : summary_config,
             "objectinfo_db" : objectinfo_db_config,
             "obj_cachesize" : obj_cachesize_config,
             }

def process_host_config( host_config = {} ):
    for bmark in list(host_config.keys()):
        hostlist = host_config[bmark].split(",")
        host_config[bmark] = hostlist
        host_config[bmark].append(bmark)
    return defaultdict( list, host_config )

def process_worklist_config( worklist_config = {} ):
    mydict = defaultdict( lambda: "NONE" )
    for bmark in list(worklist_config.keys()):
        hostlist = worklist_config[bmark].split(",")
        mydict[bmark] = hostlist
    return mydict

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "--config",
                         help = "Specify configuration filename.",
                         action = "store" )
    parser.add_argument( "--simconfig",
                         help = "Specify run configuration filename.",
                         action = "store" )
    parser.add_argument( "--mpr",
                         dest = "mprflag",
                         help = "Enable multiprocessing.",
                         action = "store_true" )
    parser.add_argument( "--single",
                         dest = "mprflag",
                         help = "Single threaded operation.",
                         action = "store_false" )
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
    parser.set_defaults( logfile = "garbology_test.log",
                         mprflag = False,
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
    # Global configurations are global to most of the scripts I've done
    global_config = configdict["global"]
    # Contains hosts that I run on
    host_config = process_host_config( configdict["hosts"] )
    # Main being the configuration for this particular scriptj
    main_config = configdict["main"]
    # Contains the object info txt/csv files from the simulator
    objectinfo_config = configdict["objectinfo"]
    # Summary of the simulator run
    summary_config = configdict["summary_config"]
    # Contains the object info from the simulator. This is exactly the same information
    # found in objectinfo text/csv files, but presented in SQLite format.
    objectinfo_db_config = configdict["objectinfo_db"]
    # How big the object DB cache size is
    obj_cachesize_config = configdict["obj_cachesize"]
    # Worklist (or what to run) for this script
    worklist_config = process_worklist_config( configdict["garbology-test-worklist"] )

    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    config_debugflag = global_config["debug"]
    # DEBUG ONLY: print "=====[ WORKLIST ]==============================================================="
    # DEBUG ONLY: pp.pprint(worklist_config)
    # DEBUG ONLY: print "=====[ OBJECTINFO_DB ]=========================================================="
    # DEBUG ONLY: pp.pprint(objectinfo_db_config)
    # DEBUG ONLY: print "================================================================================"
    return main_process( debugflag = args.debugflag,
                         mprflag = args.mprflag,
                         global_config = global_config,
                         main_config = main_config,
                         objectinfo_config = objectinfo_config,
                         host_config = host_config,
                         worklist_config = worklist_config,
                         summary_config = summary_config,
                         objectinfo_db_config = objectinfo_db_config,
                         obj_cachesize_config = obj_cachesize_config,
                         logger = logger )

if __name__ == "__main__":
    main()
