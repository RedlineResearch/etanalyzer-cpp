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

def main_process( tgtpath = None,
                  debugflag = False,
                  logger = None ):
    global pp
    exit(1000)

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "csvfile", help = "Source CSV file from basic_merge_summary.py" )
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

def process_args( args, parser ):
    #
    # Get input filename
    #
    tgtpath = args.csvfile
    try:
        if not os.path.exists( tgtpath ):
            parser.error( tgtpath + " does not exist." )
    except:
        parser.error( "invalid path name : " + tgtpath )
    # Actually open the input db/file in main_process()
    # 
    # Get logfile
    logfile = args.logfile
    logfile = "extract_types-" + os.path.basename(tgtpath) + ".log" if not logfile else logfile    
    # 
    # Get debugflag
    debugflag = args.debugflag
    return { "tgtpath" : tgtpath,
             "logfile" : logfile,
             "debugflag" : debugflag }

def main():
    parser = create_parser()
    args = parser.parse_args()
    arg_results = process_args( args, parser )
    tgtpath = arg_results["tgtpath"]
    logfile = arg_results["logfile"]
    debugflag = arg_results["debugflag"]
    # logging
    logger = setup_logger( filename = logfile,
                           debugflag = debugflag )
    #
    # Main processing
    #
    return main_process( tgtpath = tgtpath,
                         debugflag = debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()
