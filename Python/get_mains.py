from __future__ import division
# get_mains.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
# Possible useful libraries, classes and functions:
# from operator import itemgetter
# from collections import Counter
# from collections import defaultdict
#   - This one is my own library:
# from mypytools import mean, stdev, variance

# The garbology related library. Import as follows.
# Check garbology.py for other imports
# from garbology import SummaryReader, get_index

# Needed to read in *-OBJECTINFO.txt and other files from 
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time


pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "get_mains.log",
                  logger_name = 'get_mains',
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


def main_process( output = None,
                  global_config = {},
                  summary_config = {},
                  main_config = {},
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO 2016 August 7 TODO
    # This is where the summary CSV files are
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Setup stdout to file redirect TODO: Where should this comment be placed?
    # TODO: Eventually remove the following commented code related to hosts.
    # Since we're not doing mutiprocessing, we don't need this. But keep
    # it here until absolutely sure.
    # TODO: thishost = get_actual_hostname( hostname = socket.gethostname().lower(),
    # TODO:                                 host_config = host_config )
    # TODO: assert( thishost != None )
    # TODO: thishost = thishost.upper()
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
    # Take benchmarks to process from etanalyze_config
    # The benchmarks are:
    #     BENCHMARK   |   CREATE  |  DELETE   |
    #     simplelist1 |    seq    |    seq    |
    #     simplelist2 |   rand    |    seq    |
    #     simplelist3 |    seq    |    at end |
    #     simplelist4 |   rand    |    at end |
    # Where to get file?
    # Filename is in "summary_config"
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    assert( "simplelist1" in summary_config )
    assert( "simplelist2" in summary_config )
    assert( "simplelist3" in summary_config )
    assert( "simplelist4" in summary_config )
    # Give simplelist? more descriptive names
    sdict = { "SEQ-SEQ" : {}, # simplelist1
              "RAND-SEQ" : {}, # simplelist2
              "SEQ-ATEND" : {}, # simplelist3
              "RAND-ATEND" : {}, } # simplelist4
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    print "XXX:", os.path.join( cycle_cpp_dir, summary_config["simplelist1"] )
    sdict["SEQ-SEQ"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                               summary_config["simplelist1"] ) )
    sdict["RAND-SEQ"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                summary_config["simplelist2"] ) )
    sdict["SEQ-ATEND"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                 summary_config["simplelist3"] ) )
    sdict["RAND-ATEND"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                  summary_config["simplelist4"] ) )

    print "====[ Reading in the summaries ]================================================"
    for skind, mydict in sdict.iteritems():
        sreader = mydict["sreader"]
        sreader.read_summary_file()
        pp.pprint( sreader.__get_summarydict__() )
    print "DONE reading all 4."
    print "================================================================================"
    # Get summary table 1
    table1 = make_summary_table_1( sdict )
    with open( os.path.join( workdir, "simplelist-analyze.csv" ), "wb" ) as fptr:
        writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
        for row in table1:
            writer.writerow(row)
    print "get_mains.py - DONE."
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
    summary_config = config_section_map( "summary_cpp", config_parser )
    main_config = config_section_map( "simplelist-analyze", config_parser )
    # MAYBE: objectinfo_config = config_section_map( "objectinfo", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    # PROBABLY NOT: worklist_config = config_section_map( "dgroups-worklist", config_parser )
    return { "global" : global_config,
             "summary" : summary_config,
             "main" : main_config,
             # "objectinfo" : objectinfo_config,
             # "contextcount" : contextcount_config,
             # "host" : host_config,
             # "worklist" : worklist_config
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
    parser.set_defaults( logfile = "get_mains.log",
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
    summary_config = configdict["summary"]
    main_config = configdict["main"]
    # PROBABLY DELETE:
    # contextcount_config = configdict["contextcount"]
    # objectinfo_config = configdict["objectinfo"]
    # host_config = process_host_config( configdict["host"] )
    # worklist_config = process_worklist_config( configdict["worklist"] )
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         global_config = global_config,
                         summary_config = summary_config,
                         main_config = main_config,
                         logger = logger )

if __name__ == "__main__":
    main()
