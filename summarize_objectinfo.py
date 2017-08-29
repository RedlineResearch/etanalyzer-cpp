from __future__ import division
# summarize_objectinfo.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
from collections import Counter
from collections import defaultdict

# Possible useful libraries, classes and functions:
# from operator import itemgetter
#   - This one is my own library:
# from mypytools import mean, stdev, variance
from mypytools import create_work_directory

# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import ObjectInfoReader, get_index

# Needed to read in *-OBJECTINFO.txt and other files from 
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time


pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "summarize_objectinfo.log",
                  logger_name = 'summarize_objectinfo',
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
def is_array( mytype ):
    return (len(mytype) > 0) and (mytype[0] == "[")

def is_primitive_type( mytype = None ):
    return ( (mytype == "Z")     # boolean
              or (mytype == "B") # byte
              or (mytype == "C") # char
              or (mytype == "D") # double
              or (mytype == "F") # float
              or (mytype == "I") # int
              or (mytype == "J") # long
              or (mytype == "S") # short
              )

def is_primitive_array( mytype = None ):
    # Is it an array?
    if not is_array(mytype):
        return False
    else:
        return ( is_primitive_type(mytype[1:]) or
                 is_primitive_array(mytype[1:]) )
        
# TODO: Refactor out
def check_host( benchmark = None,
                worklist_config = {},
                host_config = {} ):
    thishost = socket.gethostname()
    for wanthost in worklist_config[benchmark]:
        if thishost in host_config[wanthost]:
            return True
    return False

# TODO: Refactor out
def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None


def main_process( output = None,
                  bmark = None,
                  global_config = {},
                  objectinfo_config = {},
                  objectinfo_db_config = {},
                  # worklist_config = {},
                  main_config = {},
                  fraction = 0.9,
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the OBJECTINFO files are
    assert( "cycle_cpp_dir" in global_config )
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Setup stdout to file redirect TODO: Where should this comment be placed?
    # Get the date and time to label the work directory.
    # TODO: today = date.today()
    # TODO: today = today.strftime("%Y-%m%d")
    # TODO: timenow = datetime.now().time().strftime("%H-%M-%S")
    # TODO: olddir = os.getcwd()
    # TODO: print main_config["output"]
    # TODO: os.chdir( main_config["output"] )
    # TODO: workdir = create_work_directory( work_dir = main_config["output"],
    # TODO:                                  today = today,
    # TODO:                                  timenow = timenow,
    # TODO:                                  logger = logger,
    # TODO:                                  interactive = False )
    # Take benchmarks to process from summarize-objectinfo-worklist 
    #     in ?????? configuration file.
    # Where to get file?
    # Filename is in "objectinfo_config"
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    # TODO: Go through the worklist and check to see in if objectinfo_config.
    # assert( "seq-seqdel" in objectinfo_config )
    # Give simplelist? more descriptive names
    workdir = cycle_cpp_dir
    # objdict = { bmark : {} for bmark in worklist_config.keys() }
    print "--------------------------------------------------------------------------------"
    # TODO: assert( bmark in objectinfo_db_config )
    assert( bmark in objectinfo_config )
    # db_filename = os.path.join( cycle_cpp_dir,
    #                             objectinfo_db_config[bmark] )
    # if os.path.isfile( db_filename ):
    #     print "%s: DB OK" % db_filename
    #     # objdict[bmark]["objreader"] = ObjectInfoReader( db_filename = db_filename,
    #     #                                                 useDB_as_source = True,
    #     #                                                 logger = logger )
    #     objreader = ObjectInfoReader( db_filename = db_filename,
    #                                   useDB_as_source = True,
    #                                   logger = logger )
    # else:
    #     text_filename = os.path.join( cycle_cpp_dir,
    #                                   objectinfo_config[bmark] )
    #     try:
    #         assert( os.path.isfile( text_filename ) )
    #     except:
    #         print "ERROR: Unable to find objinfo for %s[ %s ]" % (bmark, text_filename)
    #     print "%s: TEXT OK" % text_filename
    #     objreader = ObjectInfoReader( objinfo_filename = text_filename,
    #                                   useDB_as_source = False,
    #                                   logger = logger )
    text_filename = os.path.join( cycle_cpp_dir,
                                  objectinfo_config[bmark] )
    try:
        assert( os.path.isfile( text_filename ) )
    except:
        print "ERROR: Unable to find objinfo for %s[ %s ]" % (bmark, text_filename)
    print "%s: TEXT OK" % text_filename
    objreader = ObjectInfoReader( objinfo_filename = text_filename,
                                  useDB_as_source = False,
                                  logger = logger )
    print "====[ Reading in the OBJECTINFO file ]=========================================="
    objreader.read_objinfo_file()
    print "Num of objects:", len(objreader.keys())
    dsum = Counter()
    total_alloc = 0
    for objId in objreader.keys():
        rec = objreader.get_record(objId)
        dsite = objreader.get_death_context_using_record(rec)
        mysize = objreader.get_size_using_record(rec)
        total_alloc += mysize
        dsum[dsite] += mysize
    print "================================================================================"
    print "%s results:" % bmark
    sofar = 0
    target = fraction * total_alloc
    count = 0
    dsite_list = []
    for tup in dsum.most_common():
        # print "%s -> %d" % tup
        sofar += tup[1]
        count += 1
        dsite_list.append(tup[0])
        if sofar >= target:
            print "%d death sites needed for %f" % (count, fraction)
            print str(dsite_list)
            break
    print "================================================================================"
    print "DONE reading all benchmarks."
    exit(0)
    # TODO: # Get summary table 1
    # TODO: result = calculate_counts( objdict )
    # TODO: for skind, mydict in result.iteritems():
    # TODO:     print "=======[ %s ]===================================================================" % skind
    # TODO:     for dtype, mycounter in mydict.iteritems():
    # TODO:         print "    -----[ %s ]----------------------------------------------------" % dtype
    # TODO:         pp.pprint( dict(mycounter) )
    # TODO TODO TODO
    # TODO: with open( os.path.join( workdir, "simplelist-analyze.csv" ), "wb" ) as fptr:
    # TODO:     writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
    # TODO:     for row in table1:
    # TODO:         writer.writerow(row)
    # TODO: print "summarize_objectinfo.py - DONE."
    # TODO: os.chdir( olddir )
    # TODO: exit(0)

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
    main_config = config_section_map( "summarize-objectinfo", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    objectinfo_db_config = config_section_map( "objectinfo-db", config_parser )
    worklist_config = config_section_map( "summarize-objectinfo-worklist", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    # PROBABLY NOT: summary_config = config_section_map( "summary_cpp", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "objectinfo_db" : objectinfo_db_config,
             "worklist" : worklist_config
             # "summary" : summary_config,
             # "contextcount" : contextcount_config,
             # "host" : host_config,
             }

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

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "output", help = "Target output filename." )
    parser.add_argument( "--bmark",
                         help = "Specify benchmark.",
                         action = "store" )
    parser.add_argument( "--fraction",
                         help = "Specify fraction of allocation.",
                         type = float,
                         action = "store" )
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
    parser.set_defaults( logfile = "summarize_objectinfo.log",
                         fraction = None,
                         bmark = None,
                         debugflag = False,
                         config = None )
    return parser

def calculate_counts( objdict = None ):
    # TODO At end, and global result dictionaries?
    result = {}
    DIEDBY = get_index( "DIEDBY" ) # died by index
    ATTR = get_index( "STATTR" ) # stack attribute index
    TYPE = get_index( "TYPE" ) # type index
    for bmark, mydict in objdict.iteritems():
        objreader = mydict["objreader"]
        result[bmark] = {}
        rtmp = result[bmark]
        rtmp["stack_after_heap"] = Counter()
        rtmp["heap"] = Counter()
        rtmp["stack_only"] = Counter()
        rtmp["end_of_prog"] = Counter()
        rtmp["others"] = Counter()
        # TODO rtmp["stack_all"] = Counter()
        for tup in objreader.iterrecs():
            # TODO: Refactor this
            objId, rec = tup
            reason = rec[DIEDBY]
            stack_attr = rec[ATTR]
            mytype = objreader.get_type_using_typeId( rec[TYPE] )
            if reason == "S":
                # TODO: rtmp["stack_all"][mytype] += 1
                if stack_attr == "SHEAP":
                    rtmp["stack_after_heap"][mytype] += 1
                elif stack_attr == "SONLY":
                    rtmp["stack_only"][mytype] += 1
            elif reason == "H":
                rtmp["heap"][mytype] += 1
            elif reason == "E":
                rtmp["end_of_prog"][mytype] += 1
            else:
                rtmp["others"][mytype] += 1
    return result

def main():
    parser = create_parser()
    args = parser.parse_args()
    assert( args.config != None )
    # Get the configurations we need.
    configdict = process_config( args )
    global_config = configdict["global"]
    main_config = configdict["main"]
    objectinfo_config = configdict["objectinfo"]
    objectinfo_db_config = configdict["objectinfo_db"]
    # TODO: worklist_config = process_worklist_config( configdict["worklist"] )
    # TODO: print(dict(worklist_config))
    print "--------------------------------------------------------------------------------"
    # PROBABLY DELETE:
    # contextcount_config = configdict["contextcount"]
    # host_config = process_host_config( configdict["host"] )
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    # Check fraction argument
    fraction = float(args.fraction)
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         bmark = args.bmark,
                         output = args.output,
                         global_config = global_config,
                         main_config = main_config,
                         objectinfo_config = objectinfo_config,
                         objectinfo_db_config = objectinfo_db_config,
                         fraction = fraction,
                         # worklist_config = worklist_config,
                         # contextcount_config = contextcount_config,
                         # host_config = host_config,
                         logger = logger )

if __name__ == "__main__":
    main()
