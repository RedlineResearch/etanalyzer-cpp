from __future__ import division
# process_PAGC_data.py 
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
from itertools import chain
# import csv
# import networkx as nx
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
                  filename = "process_PAGC_data.log",
                  logger_name = 'process_PAGC_data',
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

def get_function_stats( data = {},
                        funcnames = {} ):
    counter = Counter( chain( *(data.keys()) ) )
    print "==============================================================================="
    for sig, garblist in data.iteritems():
        print "%s -> %s" % (str(sig), str(garblist))
    return counter

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

def get_context_pair( select, sig ):
    assert( len(sig) > 0 )
    index = 0
    while ( (index < len(sig)) and
            (select != sig[index]) ):
        index += 1
    if index < len(sig):
        caller = sig[index + 1] if (index < (len(sig) - 1)) \
                    else None
        return (sig[index], caller)
    else:
        raise RuntimeError( "Lookfing for [%s] --> invalid signature: %s" %
                            (select, str(sig)) )

def choose_functions_ver01( counter = {},
                            data = {},
                            funcnames = {} ):
    functions = defaultdict( list )
    count = 0
    for sig, garbage in data.iteritems():
        # DESIGN Decision: Use the most often called functions.
        sig_sorted = sorted( sig,
                             key = lambda x: counter[x],
                             reverse = True )
        select = None
        for funcId in sig_sorted:
            myname = funcnames[funcId]
            if is_javalib_method(myname):
                continue
            else:
                select = funcId
                break
        if select == None:
            print "--------------------------------------------------------------------------------"
            for tmp in sig_sorted:
                print funcnames[tmp]
            print "--------------------------------------------------------------------------------"
            count += 1
        else:
            cpair = get_context_pair( select, sig )
            functions[cpair].append((sig, garbage))
    print "No selection total: %d" % count
    return functions
    # Design considerations:
    #    - size of garbage
    #    - function type (library or application?)
    #    - number of times function was called.
    #    - number of garbage clusters the function is in
    #    - closest to the death event
    #    - consistency of garbage size
    #
    # Option 1:
    #   Axiom 1: It's better to use a function that gets called more.
    #   Axiom 2: It's better to use a function that generates a consistent
    #            amount of garbage. (....maybe?)
    #   Axiom 3: We'd rather use application functions rather than system library
    #            functions.

def get_data( sourcefile = None,
              data = {} ):
    # TODO: HERE TODO
    # TODO: What does the results dictionary look like?
    #       rdict = results[heapsize]
    # print "filename: %s = hsize %d" % (filename, heapsize)
    # I have 2 design possibilities here:
    # OPTION 1:
    #   - The counts reset at every garbage 
    #   - We start with reading Garbage events, keeping the total
    #   - When we hit an Exit event, we read in all Exit events before the next
    #     garbage event.
    #   - On next Garbage event, repeat as above.
    # OPTION 2:
    #   - Keep track of ALL functions.
    #   - Now that I think about it, this seems wrong.
    with open(sourcefile, "rb") as fptr:
        count = 0
        alloc_time = 0
        garbage = 0
        state = "GARBAGE"
        # TODO: exit_funcs = set()
        exit_funcs = []
        for line in fptr:
            if line == '':
                continue
            line = line.rstrip()
            rec = line.split(",")
            # DEBUG: sys.stdout.write(str(rec) + "\n")
            if rec[0] == 'A':
                # sys.stdout.write("ALLOC:\n")
                alloc_time += int(rec[1])
            elif rec[0] == 'E':
                # sys.stdout.write("EXIT:\n")
                methodId = int(rec[1])
                if state == "GARBAGE":
                    state = "EXIT"
                    assert(len(exit_funcs) == 0)
                elif state == "EXIT":
                    pass
                else:
                    raise RuntimeError("Unexpected state: %s" % state)
                # TODO: exit_funcs.add(methodId)
                exit_funcs.append(methodId)
            elif rec[0] == 'G':
                # sys.stdout.write("GARBAGE:\n")
                if state == "GARBAGE":
                    garbage += int(rec[1])
                elif state == "EXIT":
                    # Summarize the data point
                    # TODO: exit_funcs = frozenset(exit_funcs)
                    exit_funcs_tup = tuple(exit_funcs)
                    data[exit_funcs_tup].append(garbage)
                    # Reset garbage and the exit function set
                    garbage = int(rec[1])
                    # TODO: exit_funcs = set()
                    exit_funcs = []
                else:
                    raise RuntimeError("Unexpected state: %s" % state)
            elif rec[0] == 'F':
                # sys.stdout.write("FUNCTION:\n")
                methodId = int(rec[1])
                # TODO: output record
            else:
                sys.stdout.write("TODO: %s \n" % str(line))
                assert(False)
            # DEBUG count += 1
            # DEBUG if count >= 10000:
            # DEBUG     break
    sys.stdout.write("DEBUG-done: %d bytes\n" % alloc_time)
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
    output_dir = "/data/rveroy/src/trace_drop/PAGC-ANALYSIS-1/"
    # Expecting the CSV input file in the following format:
    sourcefile = "./%s-PAGC-TRACE.csv" % benchmark
    print "================================================================================"
    data = defaultdict( list )
    get_data( sourcefile = sourcefile,
              data = data )
    counter = get_function_stats( data = data,
                                  funcnames = funcnames )
    # pp.pprint( dict(counter) )
    print "================================================================================"
    functions = choose_functions_ver01( counter = counter,
                                        data = data,
                                        funcnames = funcnames )
    print "================================================================================"
    print "Number of data points: %d" % len(data.keys())
    print "================================================================================"
    print "Functions selected: TODO"
    single_count = 0
    single_garbage_total = 0
    mult_count = 0
    mult_garbage_total = 0
    for funcname, gtuplist in functions.iteritems():
        print "%s:" % str(funcname)
        assert(len(gtuplist) > 0)
        single_flag = False
        if len(gtuplist) == 1:
            single_count += 1
            single_flag = True
        else:
            mult_count += 1
        for gtup in gtuplist:
            # gtup is:
            # ( sig tuple, list of garbage sizes )
            # TODO TODO TODO
            garblist = gtup[1]
            if single_flag:
                single_garbage_total += sum(garblist)
            else:
                mult_garbage_total += sum(garblist)
            # DEBUG ONLY:
            # sys.stdout.write( "%s -> %d, %.2f, %d\n" %
            #                   (str(gtup[0]), min(garblist), mean(garblist), max(garblist)) )
    print "Single  : count[ %d ]  garbage[ %d ]" % (single_count, single_garbage_total)
    print "Multiple: count[ %d ]  garbage[ %d ]" % (mult_count, mult_garbage_total)
    print "process_PAGC_data.py - DONE."
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
    parser.set_defaults( logfile = "process_PAGC_data.log",
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
                         names_filename = args.namesfile,
                         debugflag = args.debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()
