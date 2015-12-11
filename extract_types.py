# extract_types.py 
#
import argparse
import os
import sys
import logging
import pprint
# from operator import itemgetter
from collections import Counter
import csv
import ConfigParser
from operator import itemgetter

import mypytools

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

def setup_logger( targetdir = ".",
                  filename = "extract_types.log",
                  logger_name = 'extract_types',
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

def main_process( global_config = None,
                  maincsv_config = None,
                  debugflag = False,
                  logger = None ):
    global pp
    counter = Counter()
    basic_summary_dir = global_config["basic_summary_dir"]
    for bmark, path in maincsv_config.iteritems():
        tgtpath = os.path.join( basic_summary_dir, path )
        if not os.path.isfile( tgtpath ):
            # Ignore non-existent csv files
            continue
        print "========[ %s ]==================================================================" % bmark
        with open( tgtpath, "rb" ) as fptr:
            reader = csv.reader(fptr)
            header = reader.next()
            assert( header[2] == "num_types" )
            for line in reader:
                # print line
                try:
                    x = int(line[2])
                except:
                    logger.critical( "Unable to convert field 2 into int: %s" % str(line) )
                    continue
                counter.update( [ x ] )
        countlist = sorted( dict(counter).iteritems(), key = itemgetter(1), reverse = True )
        for row in countlist:
            print "%d, %d" % row

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "csvfile", help = "Source CSV file from basic_merge_summary.py" )
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
    parser.set_defaults( logfile = "extract_types.log",
                         debugflag = False )
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

def process_config( args ):
    assert( args.config != None )
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( args.config )
    global_config = config_section_map( "global", config_parser )
    maincsv_config = config_section_map( "maincsv", config_parser )
    return ( global_config, maincsv_config )

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    assert( args.config != None )
    global_config, maincsv_config = process_config( args )
    #
    # Get input filename
    #
    tgtpath = args.csvfile
    try:
        if not os.path.exists( tgtpath ):
            parser.error( tgtpath + " does not exist." )
    except:
        parser.error( "invalid path name : " + tgtpath )
    # Get config
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( global_config = global_config,
                         maincsv_config = maincsv_config,
                         debugflag = global_config["debug"],
                         logger = logger )

if __name__ == "__main__":
    main()
