from __future__ import division
# process_PAGCFUNC_data.py 
#
import argparse
import os
import sys
import logging
import pprint
import re
from collections import Counter
from collections import defaultdict
from operator import itemgetter
import csv
from itertools import chain
# import glob
# import shutil
# from multiprocessing import Process, Manager

# Possible useful libraries, classes and functions:
#   - This one is my own library:
from mypytools import mean, stdev, variance, check_host, hex2dec

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "process_PAGCFUNC_data.log",
                  logger_name = 'process_PAGCFUNC_data',
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


# TODO: This should really be moved to garbology.py
def read_names_file( names_filename,
                     funcnames = None ):
    with open( names_filename, "rb" ) as fp:
        for line in fp:
            line = line.rstrip()
            row = line.split(" ")
            # 0 - Entity type [N,F,C,E,I,S]
            # if entity == "N":
            #     1 - method Id
            #     2 - class Id
            #     3 - class name
            #     4 - method name
            #     5 - descriptor flags TODO TODO TODO
            # else if entity == "F":
            #     TODO: IGNORE
            # else if entity == "C":
            #     TODO: IGNORE
            # else if entity == "E":
            #     TODO: IGNORE
            # else if entity == "I":
            #     TODO: IGNORE
            # else if entity == "S":
            #     TODO: IGNORE
            recordType = row[0]
            if recordType == "N":
                methodId = hex2dec(row[1])
                classId = hex2dec(row[2])
                className = row[3]
                methodName = row[4]
                desc_flags = row[5]
                # The funcnames maps:
                #      methodId -> (className, methodName) pair
                funcnames[methodId] = ( className, methodName )
            # else:
            #     pass
            #     # Ignore the rest

def is_javalib_method( methname = None ):
    return ( (methname[0:5] == "java/") or
             (methname[0:4] == "sun/") or
             (methname[0:8] == "com/sun/") or
             (methname[0:8] == "com/ibm/") )

def is_blacklisted( methname = None ):
    return False # TODO TODO
    # - Object methods
    # - init
    # - run ? (or maybe allow it?)

def get_data( sourcefile = None,
              data = [] ):
    # The CSV file source header looks like this:
    #   "methodId","total_garbage","minimum","maximum","number_times"
    # TODO: HERE TODO
    # I have NO design possibilities yet. THat's why it's a
    # TODO. :)
    # OPTION 1:
    with open(sourcefile, "rb") as fptr:
        count = 0
        zero_garbage_set = set()
        header = fptr.readline()
        for line in fptr:
            if line == '':
                continue
            line = line.rstrip()
            rec = line.split(",")
            total_garbage = int(rec[1])
            method_id = int(rec[0])
            if total_garbage == 0:
                zero_garbage_set.add( method_id )
                continue
            minimum = int(rec[2])
            maximum = int(rec[3])
            number_times = int(rec[4])
            data.append( (method_id, total_garbage, number_times, minimum, maximum) )
            # DEBUG ONLY:
            # sys.stdout.write(str(rec) + "\n")
            # count += 1
            # if count >= 10000:
            #     break
            # END DEBUG ONLY.
    sys.stdout.write("DEBUG-done: TODO\n")
    # DEBUG TODO pp.pprint(funcnames)

def main_process( benchmark = None,
                  names_filename = None,
                  debugflag = False,
                  logger = None ):
    global pp
    # TODO Create the output directory
    #-------------------------------------------------------------------------------
    funcnames = {}
    read_names_file( names_filename = names_filename,
                     funcnames = funcnames )
    output_dir = "/data/rveroy/src/trace_drop/PAGCFUNC-ANALYSIS-1/" # Expecting the CSV input file in the following format:
    sourcefile = "./%s-PAGC-FUNC.csv" % benchmark
    print "================================================================================"
    data = []
    get_data( sourcefile = sourcefile,
              data = data )
    data = sorted( data,
                   key = itemgetter(1),
                   reverse = True )
    pp.pprint(data[:15])
    print "================================================================================"
    print "Number of data points: %d" % len(data)
    print "================================================================================"
    print "process_PAGCFUNC_data.py - DONE."
    print "================================================================================"
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
    parser.add_argument( "--namesfile",
                         help = "Names file which is output from Elephant Tracks.",
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
    parser.set_defaults( logfile = "process_PAGCFUNC_data.log",
                         debugflag = False,
                         benchmark = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = args.debugflag )
    #
    # Main processing
    #
    return main_process( benchmark = args.benchmark,
                         names_filename = args.namesfile,
                         debugflag = args.debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()
