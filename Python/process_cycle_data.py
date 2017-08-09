from __future__ import division
# process_cycle_data.py 
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
# import glob
# import shutil
# from multiprocessing import Process, Manager

# Possible useful libraries, classes and functions:
#   - This one is my own library:
from mypytools import mean, stdev, variance, check_host, hex2dec, median
from mypytools import process_worklist_config, process_host_config

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

    def update_data( self, source_file ):
        global type_tuple
        d = self.data
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
                d[type_tuple]["num_of_cycle"] += 1
                cycles_size = int(rec[0])
                obj_count = int(rec[1])
                d[type_tuple]["min"] = min(obj_count, d[type_tuple]["min"])
                d[type_tuple]["max"] = max(obj_count, d[type_tuple]["max"])
                d[type_tuple]["size-list"].append( obj_count )

    def update_median( self ):
        d = self.data
        for mytype in self.data.keys():
            m = median( d[mytype]["size-list"] )
            d[mytype]["median"] = m

    def output_to_files( self,
                         csvfile = None,
                         latexfile = None ):
        """Output to CSV and LaTeX.
        """
        with open(csvfile, "wb") as csvfp, \
             open(latexfile, "wb") as texfp:
            csvwriter = csv.writer( csvfp, quoting = csv.QUOTE_NONNUMERIC )
            header = [ "type", "number of cycles", "minimum", "maximum", "median", ]
            csvwriter.writerow( header )
            d = self.data
            rowlist = []
            for mytype in d.keys():
                rec = d[mytype]
                row = [ mytype,
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

def setup_logger( targetdir = ".",
                  filename = "process_cycle_data.log",
                  logger_name = 'process_cycle_data',
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

def solve_subset_sum_poly_approx( data = [] ):
    # data is a list of tuples using this format:
    #    (method_id, total_garbage, number_times, minimum, maximum)
    # OVERVIEW of algorithm:
    #-------------------------------------------------------------
    # initialize a list S to contain one element 0.
    # for each i from 1 to N do
    #     let T <- a list consisting of xi + y, for all y in S
    #     let U <- the union of T and S
    #     sort U
    #     make S empty 
    #     let y be the smallest element of U 
    #     add y to S 
    #     for each element z of U in increasing order do
    #         // trim the list by eliminating numbers close to one another
    #         // and throw out elements greater than s
    #         if y + cs/N < z <= s:
    #             set y = z
    #             add z to S 
    # if S contains a number between (1 - c)s and s:
    #     output yes,
    # else:
    #     output no
    #-------------------------------------------------------------
    # initialize a list S to contain one element 0.
    S = [ 0, ]
    # for each i from 1 to N do
    glist = [ x[1] for x in data ]
    for i in xrange(len(glist)):
        # let T <- a list consisting of x_i + y, for all y in S
        T = [ glist[i] for y in S ]  
        # let U <- the union of T and S
        U = list(T)
        U.extend(S)
        # sort U
        U = sorted(U)
        # make S empty 
        # let y be the smallest element of U 
        y = U[0]
        # add y to S 
        S = [ y ]
        # for each element z of U in increasing order do
        #     // trim the list by eliminating numbers close to one another
        #     // and throw out elements greater than s
        #     if y + cs/N < z <= s:
        #         set y = z
        #         add z to S 

def update_mean_std( soln = [] ):
    # Returns an updated copy of the soln list
    result = []
    for rec in soln:
        mylist = []
        glist = rec[GLIST].split(";")
        print "X: %s -> %s" % (str(rec[GLIST]), str(glist))
        for x in glist:
            tmp = x.split(":")
            try:
                garbage = float(int(tmp[0]))
                mylist.append( garbage )
            except:
                pass
        mymean = mean( mylist )
        mystdev = stdev( mylist ) if len( mylist ) > 1 \
            else 0.0
        result.append( rec + (mymean, mystdev) )
    return result

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

def get_data( sourcefile = None ):
    global type_tuple
    data = []
    # The CSV file source header looks like this:
    #   "type-tuple","cycle-count","cycles-size","obj-count-min","obj-count-max","obj-count-mean","obj-count-median","singletons"
    with open(sourcefile, "rb") as fptr:
        count = 0
        header = fptr.readline()
        for line in fptr:
            if line == '':
                continue
            line = line.rstrip()
            line = re.sub( "\"(\(.*?\))\",",
                           repl = match_func,
                           string = line,
                           count = 1 )
            type_tuple = get_types( type_tuple )
            rec = line.split(",")
            # type_tuple = rec[TYPE_TUPLE]
            num_of_cycle = int(rec[NUM_OF_CYCLE])
            cycle_size = int(rec[CYCLE_SIZE])
            obj_count_min = int(rec[OBJ_COUNT_MIN])
            obj_count_max = int(rec[OBJ_COUNT_MAX])
            mean_str = rec[OBJ_COUNT_MEAN]
            mean_str = mean_str.replace('"', '')
            obj_count_mean = float(mean_str)
            obj_count_median = int(rec[OBJ_COUNT_MEDIAN])
            singletons = rec[SINGLETONS]
            newrec = (type_tuple, num_of_cycle, cycle_size,
                      obj_count_min, obj_count_max,
                      obj_count_mean, obj_count_median,
                      singletons)
            data.append( newrec )
            # DEBUG ONLY:
            #     sys.stdout.write(str(rec) + " = " + str(newrec) + "\n")
    return data

def output_to_csv( soln = [],
                   selectfp = None ):
    # Need:
    # - csv file pointer
    #   => assume called with 'with open as'
    # - solution list
    csvwriter = csv.writer( selectfp, csv.QUOTE_NONNUMERIC )
    header = [ "path_id", "number", "garbage", "garbage_list", "mean_std", "stdev_std", ]
    csvwriter.writerow( header )
    # TODO: Add GARBAGE LIST at index 3
    for rec in soln:
        row = [ rec[PATH_ID], rec[NUMBER], rec[GARBAGE], rec[GLIST],
                rec[MEAN_STD], rec[STDEV_STD], ]
        csvwriter.writerow( row )


def main_process( worklist_config = {},
                  host_config = {},
                  global_config = {},
                  summary_config = {},
                  cyclelist = {},
                  debugflag = False,
                  logger = None ):
    global pp
    global NUM_OF_CYCLE, CYCLE_SIZE, OBJ_COUNT_MIN, OBJ_COUNT_MAX, OBJ_COUNT_MEAN, OBJ_COUNT_MEDIAN, SINGLETONS
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Given file template for the cycle file, go through all
    # possible benchmarks.
    datadict = {}
    rparser = RawParser()
    for bmark in cyclelist:
        fname = "%s-cycle-summary.csv" % bmark
        rawfname = "%s-raw-cycle-summary.csv" % bmark
        absfname = os.path.join( cycle_cpp_dir, fname )
        abs_rawfname = os.path.join( cycle_cpp_dir, rawfname )
        if os.path.isfile( absfname ):
            data = get_data( absfname )
            datadict[bmark] = data
        # print "%s -> %s" % (absfname, os.path.isfile(absfname))
        if os.path.isfile( abs_rawfname ):
            rparser.update_data( abs_rawfname )
    print "Keys:", rparser.data.keys()
    print "--------------------------------------------------------------------------------"
    rparser.update_median()
    pp.pprint( rparser.data )
    csvfile = os.path.join( cycle_cpp_dir, "cycle_data-OUTPUT.csv" )
    latexfile = os.path.join( cycle_cpp_dir, "cycle_data-TABLE.tex" )
    rparser.output_to_files( csvfile = csvfile,
                             latexfile = latexfile )
    # Then aggregate.
    print "================================================================================"
    print "process_cycle_data.py - DONE."
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
    worklist_config = config_section_map( "dgroups2db-worklist", config_parser )
    main_config = config_section_map( "dgroups2db", config_parser )
    cyclelist = config_section_map( "cycle-list", config_parser )

    # reference_config = config_section_map( "reference", config_parser )
    # reverse_ref_config = config_section_map( "reverse-reference", config_parser )
    # stability_config = config_section_map( "stability-summary", config_parser )
    # obj_cachesize_config = config_section_map( "create-supergraph-obj-cachesize", config_parser )
    # edge_cachesize_config = config_section_map( "create-supergraph-edge-cachesize", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "edgeinfo" : edgeinfo_config,
             "dgroup-pickle" : dgroup_pickle_config,
             "hosts" : host_config,
             "worklist" : worklist_config,
             "summary_config" : summary_config,
             "objectinfo_db" : objectinfo_db_config,
             "dgroups2db" : dgroups2db_config,
             "cyclelist" : list(cyclelist),
             # "obj_cachesize" : obj_cachesize_config,
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
    parser.set_defaults( logfile = "process_cycle_data.log",
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
    worklist_config = process_worklist_config( configdict["worklist"] )
    host_config = process_host_config( configdict["hosts"] )
    dgroups2db_config = configdict["dgroups2db"]
    objectinfo_db_config = configdict["objectinfo_db"]
    summary_config = configdict["summary_config"]
    cyclelist = configdict["cyclelist"]
    #
    # Main processing
    #
    return main_process( worklist_config = worklist_config,
                         summary_config = summary_config,
                         global_config = global_config,
                         cyclelist = cyclelist,
                         host_config = host_config,
                         debugflag = args.debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()
