# garbology.py 
#
import argparse
import os
import sys
import logging
import pprint
import re
import ConfigParser
import csv
import datetime

pp = pprint.PrettyPrinter( indent = 4 )

#
#  PUBLIC
#
class GarbologyConfig:
    def __init__( self, config_file ):
        self.config_file_name = config_file
        self.config_parser = ConfigParser.ConfigParser()
        self.config_parser.read( config_file )
        self.process_config( config_file )

    def config_section_map( self, section, config_parser ):
        result = {}
        options = config_parser.options(section)
        for option in options:
            try:
                result[option] = config_parser.get(section, option)
            except:
                print("exception on %s!" % option)
                result[option] = None
        return result

    def process_config( self, config_file ):
        cp = self.config_parser
        self.global_cfg = self.config_section_map( "global", cp )
        self.etanalyze_cfg = self.config_section_map( "etanalyze-output", cp )
        self.cycle_analyze_cfg = self.config_section_map( "cycle-analyze", cp )
        self.edges_cfg = self.config_section_map( "edges", cp )
        self.edgeinfo_cfg = self.config_section_map( "edgeinfo", cp )
        self.objectinfo_cfg = self.config_section_map( "objectinfo", cp )
        self.summary_cfg = self.config_section_map( "summary_cpp", cp )
        self.dsites_cfg = self.config_section_map( "dsites", cp )

#
#  PRIVATE FUNCTIONS
#

def setup_logger( targetdir = ".",
                  filename = "garbology.log",
                  logger_name = 'garbology',
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

def get_trace_fp( tracefile = None,
                  logger = None ):
    if not os.path.isfile( tracefile ) and not os.path.islink( tracefile ):
        # File does not exist
        logger.error( "Unable to open %s" % str(tracefile) )
        print "Unable to open %s" % str(tracefile)
        exit(21)
    bz2re = re.compile( "(.*)\.bz2$", re.IGNORECASE )
    gzre = re.compile( "(.*)\.gz$", re.IGNORECASE )
    bz2match = bz2re.search( tracefile )
    gzmatch = gzre.search( tracefile )
    if bz2match: 
        # bzip2 file
        fp = subprocess.Popen( [ "bzcat", tracefile ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    elif gzmatch: 
        # gz file
        fp = subprocess.Popen( [ "zcat", tracefile ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    else:
        fp = open( tracefile, "r")
    return fp

#
# Main processing
#

def create_work_directory( work_dir, logger = None, interactive = False ):
    os.chdir( work_dir )
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    if os.path.isfile(today):
        print "Can not create %s as directory." % today
        exit(11)
    if not os.path.isdir( today ):
        os.mkdir( today )
    else:
        print "WARNING: %s directory exists." % today
        logger.warning( "WARNING: %s directory exists." % today )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    return today

def main_process( logger = None,
                  debugflag = False ):
    global pp
    print "===========[ DONE ]==================================================="

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "config",
                         help = "Specify configuration filename." )
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
    parser.set_defaults( logfile = "garbology.log",
                         debugflag = False,
                         config = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    assert( args.config != None )
    assert( os.path.isfile( args.config ) )
    gconfig = GarbologyConfig( args.config )
    debugflag = gconfig.global_cfg["debug"]
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = debugflag )
    #
    # Main processing
    #
    return main_process( logger = logger,
                         debugflag = debugflag )

__all__ = []

if __name__ == "__main__":
    main()
