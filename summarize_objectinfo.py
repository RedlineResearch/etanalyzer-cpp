from __future__ import division
# skeleton.py 
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
                  filename = "skeleton.log",
                  logger_name = 'skeleton',
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
                  global_config = {},
                  objectinfo_config = {},
                  worklist_config = {},
                  main_config = {},
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the OBJECTINFO files are
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Setup stdout to file redirect TODO: Where should this comment be placed?
    # Get the date and time to label the work directory.
    today = date.today()
    today = today.strftime("%Y-%m%d")
    timenow = datetime.now().time().strftime("%H-%M-%S")
    olddir = os.getcwd()
    print main_config["output"]
    os.chdir( main_config["output"] )
    workdir = create_work_directory( work_dir = main_config["output"],
                                     today = today,
                                     timenow = timenow,
                                     logger = logger,
                                     interactive = False )
    # Take benchmarks to process from summarize-objectinfo-worklist 
    #     in ?????? configuration file.
    # Where to get file?
    # Filename is in "objectinfo_config"
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    # TODO: Go through the worklist and check to see in if objectinfo_config.
    # assert( "seq-seqdel" in objectinfo_config )
    # Give simplelist? more descriptive names
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    objdict = { bmark : {} for bmark in worklist_config.keys() }
    pp.pprint(objdict)
    for bmark in objdict.keys():
        objdict[bmark]["objreader"] = ObjectInfoReader( os.path.join( cycle_cpp_dir,
                                                                      objectinfo_config[bmark] ),
                                                        logger = logger )
    print "====[ Reading in the OBJECTINFO file ]=========================================="
    for skind, mydict in objdict.iteritems():
        objreader = mydict["objreader"]
        objreader.read_objinfo_file()
    print "DONE reading all benchmarks."
    print "================================================================================"
    # Get summary table 1
    result = calculate_counts( objdict )
    for skind, mydict in result.iteritems():
        print "=======[ %s ]===================================================================" % skind
        for dtype, mycounter in mydict.iteritems():
            print "    -----[ %s ]----------------------------------------------------" % dtype
            pp.pprint( dict(mycounter) )
    exit(0)
    # TODO TODO TODO
    # Is more needed afer this?
    # TODO TODO TODO
    with open( os.path.join( workdir, "simplelist-analyze.csv" ), "wb" ) as fptr:
        writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
        for row in table1:
            writer.writerow(row)
    print "skeleton.py - DONE."
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
    main_config = config_section_map( "summarize-objectinfo", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    worklist_config = config_section_map( "summarize-objectinfo-worklist", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    # PROBABLY NOT: summary_config = config_section_map( "summary_cpp", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
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
    parser.set_defaults( logfile = "skeleton.log",
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
        print "XXX:", bmark, mydict
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
    print "DONE: calculate_counts"
    return result

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    assert( args.config != None )
    # Get the configurations we need.
    configdict = process_config( args )
    global_config = configdict["global"]
    main_config = configdict["main"]
    objectinfo_config = configdict["objectinfo"]
    worklist_config = process_worklist_config( configdict["worklist"] )
    pp.pprint(worklist_config)
    # PROBABLY DELETE:
    # contextcount_config = configdict["contextcount"]
    # host_config = process_host_config( configdict["host"] )
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
                         objectinfo_config = objectinfo_config,
                         worklist_config = worklist_config,
                         # contextcount_config = contextcount_config,
                         # host_config = host_config,
                         logger = logger )

if __name__ == "__main__":
    main()
