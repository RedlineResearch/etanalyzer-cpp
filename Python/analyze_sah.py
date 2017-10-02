from __future__ import division
# analyze_sah.py 
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
import ConfigParser
from itertools import chain
import subprocess
# import glob
# import shutil
# from multiprocessing import Process, Manager

# Possible useful libraries, classes and functions:
#   - This one is my own library:
from mypytools import mean, stdev, variance, check_host, hex2dec, median
from mypytools import process_worklist_config, process_host_config
from garbology import ObjectInfoReader

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

# GLOBALS
NUM_OF_CYCLE = 0
CYCLE_SIZE = 1
OBJ_COUNT_MIN = 2
OBJ_COUNT_MAX = 3
OBJ_COUNT_MEAN = 4
OBJ_COUNT_MEDIAN = 5
SINGLETONS = 6

type_tuple = None
def match_func( matchobj ):
    global type_tuple
    assert(matchobj)
    type_tuple = matchobj.group(0)
    return ""

def get_short_typename( mytype = "" ):
    return mytype.split("/")[-1]

def shorten_tuple_types( mytup ):
    return [ get_short_typename(x) for x in mytup ]

# TODO TODO TODO:
# This is probably not needed anymore.
# Class to parse the raw csv
# The header looks like this:
#    "type-tuple","cycles-size","obj-count","mean-age","range-age","singletons"
class RawParser:
    def __init__( self ):
        self.data = defaultdict( lambda: { "num_of_cycle" : 0,
                                           "min" : sys.maxsize,
                                           "max" : 0,
                                           "median" : 0,
                                           "size-list" : [] } )
        self.bybmark = defaultdict( lambda: { "num_of_cycle" : 0,
                                              "min" : sys.maxsize,
                                              "max" : 0,
                                              "median" : 0,
                                              "size-list" : [] } )

    def update_data( self,
                     bmark = None,
                     source_file = None ):
        global type_tuple
        d = self.data
        bm = self.bybmark
        with open( source_file, "rb" ) as fp:
            header = fp.readline()
            for line in fp:
                line = line.rstrip()
                line = re.sub( "\"(\(.*?\))\",",
                               repl = match_func,
                               string = line,
                               count = 1 )
                type_tuple = get_types( type_tuple )
                print "%s -> %s" % (str(type_tuple), str(line)) 
                rec = line.split(",")
                # By type signature
                d[type_tuple]["num_of_cycle"] += 1
                cycles_size = int(rec[0])
                obj_count = int(rec[1])
                d[type_tuple]["min"] = min(obj_count, d[type_tuple]["min"])
                d[type_tuple]["max"] = max(obj_count, d[type_tuple]["max"])
                d[type_tuple]["size-list"].append( obj_count )
                # Slicing by benchmark
                bm[bmark]["num_of_cycle"] += 1
                bm[bmark]["min"] = min(obj_count, bm[bmark]["min"])
                bm[bmark]["max"] = max(obj_count, bm[bmark]["max"])
                bm[bmark]["size-list"].append( obj_count )

    def update_median( self ):
        d = self.data
        for mytype in self.data.keys():
            m = median( d[mytype]["size-list"] )
            d[mytype]["median"] = m

    def output_to_files( self,
                         csvfile = None,
                         latexfile = None,
                         bm_csvfile = None,
                         bm_latexfile = None,
                         logger = None ):
        """Output to CSV and LaTeX.
        """
        #---------------------------------------------------------------------
        # First the tables sliced by type signature:
        with open(csvfile, "wb") as csvfp:
            csvwriter = csv.writer( csvfp, quoting = csv.QUOTE_NONNUMERIC )
            header = [ "type", "number_of_cycles", "minimum", "maximum", "median", ]
            csvwriter.writerow( header )
            d = self.data
            rowlist = []
            for mytype_tup in d.keys():
                rec = d[mytype_tup]
                mytype_tup = shorten_tuple_types( mytype_tup )
                row = [ " ".join( mytype_tup ),
                        rec["num_of_cycle"],
                        rec["min"],
                        rec["max"],
                        rec["median"], ]
                rowlist.append( row )
            rowlist = sorted( rowlist,
                              key = itemgetter(1),
                              reverse = True )
            for row in rowlist:
                csvwriter.writerow( row )
        # run ismm2017-plot.R
        output_table( rscript_path = "/data/rveroy/bin/Rscript",
                      table_script = "/data/rveroy/pulsrc/etanalyzer-cpp/Rgraph/tables-onward2017.R",
                      csvfile = csvfile, # csvfile is the input
                      latexfile = latexfile,
                      primary = "Type",
                      logger = logger )
        #---------------------------------------------------------------------
        # Next the tables sliced by type benchmark:
        with open(bm_csvfile, "wb") as bm_csvfp:
            bm_csvwriter = csv.writer( bm_csvfp, quoting = csv.QUOTE_NONNUMERIC )
            header = [ "benchmark", "number_of_cycles", "minimum", "maximum", "median", ]
            bm_csvwriter.writerow( header )
            bm = self.bybmark
            rowlist = []
            for bmark in bm.keys():
                rec = bm[bmark]
                row = [ bmark,
                        rec["num_of_cycle"],
                        rec["min"],
                        rec["max"],
                        rec["median"], ]
                rowlist.append( row )
            rowlist = sorted( rowlist,
                              key = itemgetter(1),
                              reverse = True )
            for row in rowlist:
                bm_csvwriter.writerow( row )
        output_table( rscript_path = "/data/rveroy/bin/Rscript",
                      table_script = "/data/rveroy/pulsrc/etanalyzer-cpp/Rgraph/tables-onward2017.R",
                      csvfile = bm_csvfile, # bm_csvfile is the input
                      latexfile = bm_latexfile,
                      primary = "Benchmark",
                      logger = logger )
# END: TODO TODO TODO.

def setup_logger( targetdir = ".",
                  filename = "analyze_sah.log",
                  logger_name = 'analyze_sah',
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

def get_types( typestr = "" ):
    tstr = typestr.replace( "(", '' )
    tstr = tstr.replace( ")", '' )
    tstr = tstr.replace( "u'", '' )
    tstr = tstr.replace( "'", '' )
    tstr = tstr.replace( '"', '' )
    tstr = re.sub( ",*$", "", tstr )
    tstr = re.sub( "\s*", "", tstr )
    types = tstr.split( "," )
    result = [ re.sub( "^\[L", "[", t ) for t in types ]
    result = tuple( sorted( [ re.sub( "^L", "", t ) for t in result ], reverse = True ) )
    return result


def summarize_stack_after_heap( objectinfo = {} ):
    result = {}
    for objId in objectinfo.keys():
        if objectinfo.died_by_stack_after_heap( objId ):
            result[ objId ] = {}
    return result

def main_process( bmark = None,
                  host_config = {},
                  global_config = {},
                  summary_config = {},
                  objectinfo_db_config = {},
                  obj_cachesize = 5000000,
                  debugflag = False,
                  logger = None ):
    global pp
    # TODO: global NUM_OF_CYCLE, CYCLE_SIZE, OBJ_COUNT_MIN, OBJ_COUNT_MAX, OBJ_COUNT_MEAN, OBJ_COUNT_MEDIAN, SINGLETONS
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Given file template for the cycle file, go through all
    # possible benchmarks.
    datadict = {}
    print " - Using objectinfo DB:"
    db_filename = os.path.join( cycle_cpp_dir,
                                objectinfo_db_config[bmark] )
    print "XXX:", db_filename
    objectinfo = ObjectInfoReader( useDB_as_source = True,
                                   db_filename = db_filename,
                                   cachesize = obj_cachesize,
                                   logger = logger )
    objectinfo.read_objinfo_file()
    data = summarize_stack_after_heap( objectinfo )
    print "RESULT:"
    pp.pprint(data)
    # TODO: HERE 1 October 2017
    print "DONE: DEBUG[ %s ]: %d" % (bmark, len(objectinfo))
    exit(1111)
    csvfile = os.path.join( cycle_cpp_dir, "cycle_data-OUTPUT.csv" )
    latexfile = os.path.join( cycle_cpp_dir, "cycle_data-TABLE.tex" )
    bm_csvfile = os.path.join( cycle_cpp_dir, "cycle_data-OUTPUT-by-bmark.csv" )
    bm_latexfile = os.path.join( cycle_cpp_dir, "cycle_data-TABLE-by-bmark.tex" )
    rparser.output_to_files( csvfile = csvfile,
                             latexfile = latexfile,
                             bm_csvfile = bm_csvfile,
                             bm_latexfile = bm_latexfile,
                             logger = logger )
    # Then aggregate.
    print "================================================================================"
    print "analyze_sah.py - DONE."
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
    host_config = config_section_map( "hosts", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    dgroup_pickle_config = config_section_map( "clean-dgroup-pickle", config_parser )
    summary_config = config_section_map( "summary-cpp", config_parser )
    objectinfo_db_config = config_section_map( "objectinfo-db", config_parser )
    dgroups2db_config = config_section_map( "dgroups2db", config_parser )
    # worklist_config = config_section_map( "dgroups2db-worklist", config_parser )
    main_config = config_section_map( "dgroups2db", config_parser )
    obj_cachesize_config = config_section_map( "create-supergraph-obj-cachesize", config_parser )
    # TODO: cyclelist = config_section_map( "cycle-list", config_parser )

    # TODO: 1 Oct 2017
    #     : Very likely will need the following ---
    # reference_config = config_section_map( "reference", config_parser )
    # reverse_ref_config = config_section_map( "reverse-reference", config_parser )
    # stability_config = config_section_map( "stability-summary", config_parser )
    # TODO: Probably DON'T need the following ---
    # edge_cachesize_config = config_section_map( "create-supergraph-edge-cachesize", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "edgeinfo" : edgeinfo_config,
             "dgroup-pickle" : dgroup_pickle_config,
             "hosts" : host_config,
             # "worklist" : worklist_config,
             "summary_config" : summary_config,
             "objectinfo_db" : objectinfo_db_config,
             "dgroups2db" : dgroups2db_config,
             "cachesize" : obj_cachesize_config,
             # TODO: "cyclelist" : list(cyclelist),
             # "edge_cachesize" : edge_cachesize_config,
             # "reference" : reference_config,
             # "reverse-reference" : reverse_ref_config,
             # "stability" : stability_config,
             }

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "--config",
                         help = "Specify configuration filename.",
                         action = "store" )
    parser.add_argument( "--benchmark",
                         help = "Specify benchmark to analyze.",
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
    parser.set_defaults( logfile = "analyze_sah.log",
                         config = None,
                         debugflag = False,
                         benchmark = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = args.debugflag )
    # Get the configurations we need.
    configdict = process_config( args )
    global_config = configdict["global"]
    main_config = configdict["main"]
    # worklist_config = process_worklist_config( configdict["worklist"] )
    host_config = process_host_config( configdict["hosts"] )
    dgroups2db_config = configdict["dgroups2db"]
    objectinfo_db_config = configdict["objectinfo_db"]
    summary_config = configdict["summary_config"]
    cachesize_config = configdict["cachesize"]
    # TODO: 1-Oct-2017- Remove: TODO: cyclelist = configdict["cyclelist"]
    #
    # Main processing
    #
    return main_process( bmark = args.benchmark,
                         summary_config = summary_config,
                         global_config = global_config,
                         objectinfo_db_config = objectinfo_db_config,
                         host_config = host_config,
                         obj_cachesize = int(cachesize_config[args.benchmark]),
                         debugflag = args.debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()


#================================================================================
# TODO: Commented out because I may need it
# TODO:
#
# def output_to_csv( soln = [],
#                    selectfp = None ):
#     # Need:
#     # - csv file pointer
#     #   => assume called with 'with open as'
#     # - solution list
#     csvwriter = csv.writer( selectfp, csv.QUOTE_NONNUMERIC )
#     header = [ "path_id", "number", "garbage", "garbage_list", "mean_std", "stdev_std", ]
#     csvwriter.writerow( header )
#     # TODO: Add GARBAGE LIST at index 3
#     for rec in soln:
#         row = [ rec[PATH_ID], rec[NUMBER], rec[GARBAGE], rec[GLIST],
#                 rec[MEAN_STD], rec[STDEV_STD], ]
#         csvwriter.writerow( row )
# 
# def output_table( rscript_path = None,
#                   table_script = None,
#                   csvfile = None,
#                   latexfile = None,
#                   primary = "",
#                   logger = None ):
#     assert( os.path.isfile( rscript_path ) )
#     assert( os.path.isfile( table_script ) )
#     assert( os.path.isfile( csvfile ) )
#     cmd = [ rscript_path, # The Rscript executable
#             table_script, # Our R script that generates the table
#             csvfile, # The csv file that contains the data
#             latexfile, # LaTeX table file output
#             primary, ] # The primary key to slice by
#     print "Running R table script on %s -> %s" % (csvfile, latexfile)
#     logger.debug( "[ %s ]" % str(cmd) )
#     rproc = subprocess.Popen( cmd,
#                               stdout = subprocess.PIPE,
#                               stdin = subprocess.PIPE,
#                               stderr = subprocess.PIPE )
#     result = rproc.communicate()
#     # Send debug output to logger
#     logger.debug("--------------------------------------------------------------------------------")
#     for x in result:
#         logger.debug(str(x))
#         print "XXX:", str(x)
#     logger.debug("--------------------------------------------------------------------------------")
# 
# def get_data( sourcefile = None ):
#     global type_tuple
#     data = []
#     # The CSV file source header looks like this:
#     #   "type-tuple","cycle-count","cycles-size","obj-count-min","obj-count-max","obj-count-mean","obj-count-median","singletons"
#     with open(sourcefile, "rb") as fptr:
#         count = 0
#         header = fptr.readline()
#         for line in fptr:
#             if line == '':
#                 continue
#             line = line.rstrip()
#             # The 'match_func' saves the match in
#             # the global variable 'type_tuple'
#             line = re.sub( "\"(\(.*?\))\",",
#                            repl = match_func,
#                            string = line,
#                            count = 1 )
#             type_tuple = get_types( type_tuple )
#             rec = line.split(",")
#             # type_tuple = rec[TYPE_TUPLE]
#             num_of_cycle = int(rec[NUM_OF_CYCLE])
#             cycle_size = int(rec[CYCLE_SIZE])
#             obj_count_min = int(rec[OBJ_COUNT_MIN])
#             obj_count_max = int(rec[OBJ_COUNT_MAX])
#             mean_str = rec[OBJ_COUNT_MEAN]
#             mean_str = mean_str.replace('"', '')
#             obj_count_mean = float(mean_str)
#             obj_count_median = int(rec[OBJ_COUNT_MEDIAN])
#             singletons = rec[SINGLETONS]
#             newrec = (type_tuple, num_of_cycle, cycle_size,
#                       obj_count_min, obj_count_max,
#                       obj_count_mean, obj_count_median,
#                       singletons)
#             data.append( newrec )
#             # DEBUG ONLY:
#             #     sys.stdout.write(str(rec) + " = " + str(newrec) + "\n")
#     return data
# 
