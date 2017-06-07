from __future__ import division
# process_PAGCFUNC_data-ver2.py 
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

# GLOBALS
CALLEE_ID = 0
CALLER_ID = 1
GARBAGE = 2
MINIMUM = 3
MAXIMUM = 4
NUMBER = 5
GLIST = 6

def setup_logger( targetdir = ".",
                  filename = "process_PAGCFUNC_data-ver2.log",
                  logger_name = 'process_PAGCFUNC_data-ver2',
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

def solve_subset_sum_naive_ver2( data = [],
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
    #   "callee_id","caller_id","total_garbage","minimum","maximum","number_times","garbage_list"
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
            callee_id = int(rec[CALLEE_ID])
            caller_id = int(rec[CALLER_ID])
            total_garbage = int(rec[GARBAGE])
            if total_garbage == 0:
                zero_garbage_set.add( (callee_id, caller_id) )
                continue
            minimum = int(rec[MINIMUM])
            maximum = int(rec[MAXIMUM])
            number_times = int(rec[NUMBER])
            glist = rec[GLIST]
            data.append( (callee_id, caller_id, total_garbage, number_times, minimum, maximum, glist) )
            # DEBUG ONLY:
            # sys.stdout.write(str(rec) + "\n")
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
    header = [ "callee_id", "caller_id", "number", "garbage", "garbage_list", ]
    csvwriter.writerow( header )
    # TODO: Add GARBAGE LIST at index 3
    for rec in soln:
        row = [  rec[CALLEE_ID], rec[CALLER_ID], rec[NUMBER], rec[GARBAGE], rec[GLIST] ]
        csvwriter.writerow( row )

def main_process( benchmark = None,
                  names_filename = None,
                  target = None,
                  epsilon = None,
                  debugflag = False,
                  logger = None ):
    global pp
    # TODO Create the output directory
    #-------------------------------------------------------------------------------
    funcnames = {}
    read_names_file( names_filename = names_filename,
                     funcnames = funcnames )
    output_dir = "/data/rveroy/src/trace_drop/PAGCFUNC-ANALYSIS-2/" # Expecting the CSV input file in the following format:
    sourcefile = "./%s-PAGC-FUNC.csv" % benchmark
    print "================================================================================"
    data = []
    get_data( sourcefile = sourcefile,
              data = data )
    data = sorted( data,
                   key = itemgetter(2), # Sort on garbage total
                   reverse = True )
    # DEBUG: pp.pprint(data[:15])
    # TODO HERE 3 June 2017 TODO TODO
    result = solve_subset_sum_naive( data = data,
                                     target = target,
                                     epsilon = epsilon )
    # Choose the appropriate mean here? Or in the previous
    # simulator?
    soln = result["solution"]
    over_soln = result["over_solution"]
    print "================================================================================"
    print "STRICT Solution:"
    if len(soln) > 0:
        print "Solution EXISTS:"
        total = 0
        for tup in soln:
            total += tup[GARBAGE]
            print "m[ %d, %d ] = %d : %s" % (tup[CALLEE_ID], tup[CALLER_ID], tup[GARBAGE], tup[GLIST])
        print "Solution total = %d" % total
        print "Target         = %d" % target
        selectfile = "./%s-PAGC-FUNC-select-1.csv" % benchmark
        with open( selectfile, "wb" ) as selectfp:
            # TODO: Calculate standard mean
            #   - TODO: Are there any other means that we need to do? Or want to do?
            # Create new solution data structure? Or can we just modify in place?
            # Or does it not even matter if we can modify in place?
            output_to_csv( soln = soln,
                           selectfp = selectfp )
    else:
        print "NO SOLUTION."
        pp.pprint(soln)
    print "================================================================================"
    print "OVER Solution:"
    if len(over_soln) > 0:
        best = over_soln.pop(0)
        minimum = sum( [ x[GARBAGE] for x in best ] )
        for cand in over_soln[1:]:
            newmin = sum( [ x[GARBAGE] for x in cand ] )
            if newmin < minimum:
                minimum = newmin
                best = cand
        for tup in best:
            print "m[ %d ] = %d" % (tup[CALLEE_ID], tup[CALLER_ID], tup[GARBAGE])
        print "Solution total = %d" % minimum
        print "Target         = %d" % target
    else:
        print "NO OVER SOLUTION."
        pp.pprint(over_soln)
    print "================================================================================"
    print "process_PAGCFUNC_data-ver2.py - DONE."
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
    parser.add_argument( "--target",
                         type = int,
                         help = "Target garbage total.",
                         action = "store" )
    parser.add_argument( "--epsilon",
                         type = int,
                         help = "Target garbage total.",
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
    parser.set_defaults( logfile = "process_PAGCFUNC_data-ver2.log",
                         debugflag = False,
                         benchmark = None,
                         target = None,
                         epsilon = 0 )
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
                         target = args.target,
                         epsilon = args.epsilon,
                         debugflag = args.debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()
