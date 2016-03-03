# run_dgroup_plots.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
from operator import itemgetter

import subprocess
# TODO: Use twisted
import datetime

pp = pprint.PrettyPrinter( indent = 4 )


def setup_logger( targetdir = ".",
                  filename = "run_dgroup_plots.log",
                  logger_name = 'run_dgroup_plots',
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

def run_Rscript( data = None,
                 bmark = None,
                 outdir = None,
                 Rscript = None,
                 plot_script = None ):
    cmd = [ Rscript, plot_script, data, bmark, outdir ]
    print "%s" % str(cmd)
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    result = None
    return result

#
# Main processing
#

def main_process( output = None,
                  benchmarks_config = None,
                  dgroups_config = None,
                  global_config = None,
                  logger = None,
                  debugflag = False ):
    global pp
    print "GLOBAL:"
    pp.pprint(global_config)
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    srcdir = global_config["dgroup_dir"]
    print "srcdir:", srcdir
    outdir = global_config["dgroup_outdir"]
    Rscript = global_config["rscript"]
    plot_script = global_config["plot_script"]
    assert(os.path.isdir(outdir))
    assert(os.path.isdir(srcdir))
    assert(os.path.isfile(Rscript))
    assert(os.path.isfile(plot_script))
    for bmark in benchmarks_config.iterkeys():
        tgtfile = os.path.join( srcdir, dgroups_config[bmark] )
        print bmark, "->", dgroups_config[bmark], "=", os.path.isfile( tgtfile )
        run_Rscript( data = tgtfile,
                     bmark = bmark,
                     outdir = outdir,
                     Rscript = Rscript,
                     plot_script = plot_script )
    # HERE: TODO
    print "===========[ DONE ]==================================================="
    exit(0)

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
    parser.set_defaults( logfile = "run_dgroup_plots.log",
                         debugflag = False,
                         config = None )
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
    benchmarks_config = config_section_map( "benchmarks", config_parser )
    dgroups_config = config_section_map( "dgroups", config_parser )
    return ( global_config, benchmarks_config, dgroups_config, )

def main():
    parser = create_parser()
    args = parser.parse_args()
    assert( args.config != None )
    configparser = ConfigParser.ConfigParser()
    global_config, benchmarks_config, dgroups_config = process_config( args )
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         benchmarks_config = benchmarks_config,
                         dgroups_config = dgroups_config,
                         global_config = global_config,
                         logger = logger )

if __name__ == "__main__":
    main()
