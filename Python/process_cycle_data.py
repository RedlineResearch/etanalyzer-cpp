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
from mypytools import mean, stdev, variance, check_host, hex2dec
from mypytools import process_worklist_config, process_host_config

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

# GLOBALS
TYPE_TUPLE = 0
NUM_OF_CYCLE = 1
CYCLE_LEN = 2
OBJ_COUNT_MIN = 3
OBJ_COUNT_MAX = 4
OBJ_COUNT_MEAN = 5
OBJ_COUNT_MEDIAN = 6
SINGLETONS = 7


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

def solve_subset_sum_naive_ver3( data = [],
                                 target = None,
                                 epsilon = 0 ):
    # Print out problem state:
    print "Target: %d" % target
    print "Epsilon: %d" % epsilon
    # Assume data is sorted, increasing.
    # data can also have duplicates.
    solutions = []
    soln = []
    copy = list(data)
    within_target = lambda x: ( (x >= target - epsilon) and (x <= target + epsilon) )
    total = 0
    done = False
    while not done:
        assert( len(soln) == 0 )
        assert( len(copy) > 0 )
        soln.append( copy.pop(0) )
        total = soln[0][GARBAGE]
        # print "DEBUG: %s" % str(soln)
        others = list(copy)
        while len(others) > 0:
            cand = others.pop(0)
            total += cand[GARBAGE]
            soln.append( cand )
            if within_target(total):
                break
            elif total > (target + epsilon):
                # We can shortcircuit this branch now since data is sorted
                # Remove the 2 most recently added elements.
                # - last added:
                total -= cand[GARBAGE]
                soln.pop()
                # - next to last, if not empty
                # Since the soln list may be empty:
                if len(soln) > 0:
                    cand = soln.pop()
                    total -= cand[GARBAGE]
            # else we can go on and add more
        if not within_target(total):
            soln = []
            total = 0
            done = (len(copy) > 0)
        else:
            done = True
    if not within_target(total):
        return []
    else:
        return soln

def solve_subset_sum_naive( data = [],
                            target = None,
                            epsilon = 0 ):
    # TODO: Instead of a single method Id, we now have a context pair (callee, caller)
    # Print out problem state:
    print "Target: %d" % target
    print "Epsilon: %d" % epsilon
    # Assume data is sorted, increasing.
    # data can also have duplicates.
    soln = []
    soln_total = 0
    over_soln = []
    closest_under_soln = None
    total_under_soln = 0
    copy = list(data)
    within_target = lambda x: ( (x >= target - epsilon) and (x <= target + epsilon) )
    total = 0
    done = False
    while not done:
        # Clear solution
        soln = []
        total = 0
        assert( len(copy) > 0 )
        saved_under = False
        cand = copy.pop(0)
        # TODO: if cand < epsilon:
        # TODO:     break
        soln.append( cand )
        total = cand[GARBAGE]
        # print "DEBUG: %s" % str(soln)
        others = list(copy)
        while len(others) > 0:
            cand = others.pop(0)
            total += cand[GARBAGE]
            soln.append( cand )
            if within_target(total):
                done = True
                soln_total = total
                break
            elif total > (target + epsilon):
                # Save this solution anyway:
                over_soln.append( list(soln) )
                # We CAN'T shortcircuit this branch since data is sorted in increasing order.
                # Remove the 2 most recently added elements.
                # - last added:
                total -= cand[GARBAGE]
                soln.pop()
                # Put this back into others
                others.insert( 0, cand )
                # - next to last, if not empty
                # Since the soln list may be empty:
                if len(soln) > 0:
                    if total < (target - epsilon):
                        # Save it if it is closer to target than current approximation
                        if (target - total) < (target - total_under_soln):
                            closest_under_soln = soln
                            total_under_soln = total
                    cand = soln.pop()
                    total -= cand[GARBAGE]
        if not done:
            done = (len(copy) > 0)

    if not within_target(total):
        return { "solution" : [],
                 "over_solution" : over_soln, }
    else:
        return { "solution" : soln,
                 "over_solution" : over_soln, }

def get_data( sourcefile = None,
              data = [] ):
    # The CSV file source header looks like this:
    #   "path_id","total_garbage","minimum","maximum","number_times","garbage_list"
    # TODO. :)
    with open(sourcefile, "rb") as fptr:
        count = 0
        zero_garbage_set = set()
        header = fptr.readline()
        for line in fptr:
            if line == '':
                continue
            line = line.rstrip()
            rec = line.split(",")
            path_id = int(rec[PATH_ID])
            total_garbage = int(rec[GARBAGE])
            if total_garbage == 0:
                zero_garbage_set.add( path_id )
                continue
            minimum = int(rec[MINIMUM])
            maximum = int(rec[MAXIMUM])
            number_times = int(rec[NUMBER])
            glist = rec[GLIST]
            data.append( (path_id, total_garbage, minimum, maximum, number_times, glist) )
            # DEBUG ONLY:
            # parsed = "[%d, %d, %d, %d, %d -- %s]" % ( path_id, total_garbage,
            #                                           minimum, maximum, number_times, glist )
            # sys.stdout.write(str(rec) + " = " + parsed + "\n")
            # count += 1
            # if count >= 10000:
            #     break
            # END DEBUG ONLY.
        # sys.stdout.write("DEBUG-done: TODO\n")

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
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Given file template for the cycle file, go through all
    # possible benchmarks.
    pp.pprint( cyclelist )
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
