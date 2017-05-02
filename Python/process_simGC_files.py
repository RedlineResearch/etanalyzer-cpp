from __future__ import division
# process_simGC_files.py 
#
import argparse
import os
import sys
import glob
import logging
import pprint
import re
import ConfigParser
from collections import Counter
from collections import defaultdict
from operator import itemgetter
# import csv
# import networkx as nx
# import shutil
# from multiprocessing import Process, Manager

# Possible useful libraries, classes and functions:
#   - This one is my own library:
from mypytools import mean, stdev, variance, check_host

# For timestamping directories and files.
from datetime import datetime, date
import time


pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "process_simGC_files.log",
                  logger_name = 'process_simGC_files',
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


# Main processing
#
def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None
    
#================================================================================
#================================================================================

def filename2tuple( worklist = [] ):
    result = []
    for fname in worklist:
        ftmp = fname[2:]
        assert(ftmp[-4:] == ".csv")
        ftmp = ftmp[:-4]
        bmark, heapsize = ftmp.split("-")
        heapsize = int(heapsize)
        result.append( (fname, heapsize) )
    return result

def get_output( worklist = [],
                results = {} ):
    for filename, heapsize in worklist:
        assert( heapsize not in results )
        results[heapsize] = { "total_objects" : 0,
                              "total_alloc" : 0,
                              "number_collections" : 0,
                              "mark_total" : 0,
                              "mark_saved" : 0, }
        rdict = results[heapsize]
        # print "filename: %s = hsize %d" % (filename, heapsize)
        no_GC_flag = False
        with open(filename, "rb") as fptr:
            for line in fptr:
                if line != '':
                    # If time stamp ignore
                    if ( "  Method time:" in line or
                         "Done at time" in line or
                         "ERROR:" in line or
                         "Memory size:" in line or
                         "initialize_special_group:" in line ):
                        continue
                    elif "GC[" in line:
                        # TODO TODO TODO
                        # Maybe check GC count just in case?
                        # Like a last GC count.
                        pass
                    else:
                        tup  = line.split(":")
                        if len(tup) != 2:
                            continue
                        key, val = tup
                        try:
                            val = int(re.sub("\s", "", val))
                        except:
                            continue
                        val = val if val >= 0 else 0
                        if "Total objects" in key:
                            rdict["total_objects"] = val
                        elif "Total allocated in bytes" in key:
                            rdict["total_alloc"] = val
                        elif "Number of collections" in key:
                            rdict["number_collections"] = val
                            no_GC_flag = (val == 0)
                        elif "Mark total" in key:
                            rdict["mark_total"] = val
                        elif "- mark saved" in key:
                            rdict["mark_saved"] = val
                        elif "- total alloc" in key:
                            assert( val == rdict["total_alloc"] )
                        else:
                            sys.stderr.write( "Unexpected line: %s" % line )
                            sys.stdout.flush()
                        # sys.stdout.write( line )
        if no_GC_flag:
            break

def main_process( benchmark = None,
                  debugflag = False,
                  logger = None ):
    global pp
    output_dir = "/data/rveroy/src/trace_drop/GC-SIM-OUTPUT/"
    worklist = glob.glob("./%s-*.csv" % benchmark)
    worklist = sorted( filename2tuple( worklist ),
                       key = itemgetter(1) )
    print "================================================================================"
    print "================================================================================"
    results = {}
    get_output( worklist = worklist,
                results = results )
    pp.pprint( results )
    print "================================================================================"
    print "Number of data points: %d" % len(results.keys())
    print "process_simGC_files.py - DONE."
    exit(100)

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
    main_config = config_section_map( "create-supergraph", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    # TODO dgroup_config = config_section_map( "etanalyze-output", config_parser )
    dgroup_pickle_config = config_section_map( "clean-dgroup-pickle", config_parser )
    worklist_config = config_section_map( "create-supergraph-worklist", config_parser )
    obj_cachesize_config = config_section_map( "create-supergraph-obj-cachesize", config_parser )
    edge_cachesize_config = config_section_map( "create-supergraph-edge-cachesize", config_parser )
    reference_config = config_section_map( "reference", config_parser )
    reverse_ref_config = config_section_map( "reverse-reference", config_parser )
    stability_config = config_section_map( "stability-summary", config_parser )
    summary_config = config_section_map( "summary-cpp", config_parser )
    objectinfo_db_config = config_section_map( "objectinfo-db", config_parser )
    dgroups2db_config = config_section_map( "dgroups2db", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "edgeinfo" : edgeinfo_config,
             "dgroup-pickle" : dgroup_pickle_config,
             "hosts" : host_config,
             "create-supergraph-worklist" : worklist_config,
             "reference" : reference_config,
             "reverse-reference" : reverse_ref_config,
             "stability" : stability_config,
             "summary_config" : summary_config,
             "objectinfo_db" : objectinfo_db_config,
             "obj_cachesize" : obj_cachesize_config,
             "edge_cachesize" : edge_cachesize_config,
             "dgroups2db" : dgroups2db_config,
             # "contextcount" : contextcount_config,
             # "host" : host_config,
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
    parser.add_argument( "--benchmark",
                         help = "Benchmark name for filename base.",
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
    parser.set_defaults( logfile = "process_simGC_files.log",
                         debugflag = False,
                         benchmark = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = args.debugflag )
    #
    # Main processing
    #
    return main_process( benchmark = args.benchmark,
                         debugflag = args.debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()