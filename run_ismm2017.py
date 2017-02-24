# run_ismm2017.py 
#
import argparse
import os
import sys
import time
import logging
import sqorm
import cPickle
import pprint
import re
from operator import itemgetter
from collections import Counter
import StringIO
import csv
import subprocess
import datetime
import heapq
import tarfile
from tempfile import mkdtemp
from itertools import combinations
from shutil import move, rmtree
from glob import glob

from mypytools import mean, stdev, variance

from garbology import SummaryReader

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

ATIME = 0
DTIME = 1
SIZE = 2
TYPE = 3
REASON = 4

def setup_logger( targetdir = ".",
                  filename = "run_ismm2017.log",
                  logger_name = 'run_ismm2017',
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

def debug_lifetimes( G, cycle, bmark, logger ):
    global pp
    for x in cycle:
        if G.node[x]["lifetime"] <= 0:
            n = G.node[x]
            # print "XXX %s: [ %d - %s ] lifetime: %d" % \
            #     (bmark, x, n["type"], n["lifetime"])
            logger.critical( "XXX: [ %d - %s ] lifetime: %d" %
                             (x, n["type"], n["lifetime"]) )

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


def row_to_string( row ):
    result = None
    strout = StringIO.StringIO()
    csvwriter = csv.writer(strout)
    # Is the list comprehension necessary? Doesn't seem like it.
    csvwriter.writerow( [ x for x in row ] )
    result = strout.getvalue()
    strout.close()
    return result.replace("\r", "")

def render_graphs( rscript_path = None,
                   barplot_script = None,
                   csvfile = None,
                   graph_dir = None,
                   logger = None ):
    assert( os.path.isfile( rscript_path ) )
    assert( os.path.isfile( barplot_script ) )
    assert( os.path.isfile( csvfile ) )
    assert( os.path.isdir( graph_dir ) )
    cmd = [ rscript_path, # The Rscript executable
            barplot_script, # Our R script that generates the plots/graphs
            csvfile, # The csv file that contains the data
            graph_dir, ] # Where to place the PDF output files
    print "Running R barplot script  on %s -> directory %s" % (csvfile, graph_dir)
    logger.debug( "[ %s ]" % str(cmd) )
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    # Send debug output to logger
    logger.debug("--------------------------------------------------------------------------------")
    for x in result:
        logger.debug(str(x))
        print "XXX:", str(x)
    logger.debug("--------------------------------------------------------------------------------")

# Outputs all the benchmarks and the related information
def output_summary( output_path_ALL = None,
                    summary = None ):
    with open(output_path_ALL, "wb") as fpALL:
        csvwriter_ALL = csv.writer(fpALL)
        header = [ "benchmark", "total_objects",
                   "died_by_stack_size", "died_by_heap_size", "died_at_end_size",
                   "died_by_stack_after_heap_size", "died_by_stack_only_size",
                   "max_live_size",
                   ]
        csvwriter_ALL.writerow( header )
        for bmark, d in summary.iteritems():
            row = [ bmark, d["number_of_objects"],
                    d["died_by_stack_size"], d["died_by_heap_size"], d["died_at_end_size"],
                    d["died_by_stack_after_heap_size"], d["died_by_stack_only_size"],
                    d["max_live_size"],
                    ]
            csvwriter_ALL.writerow( row )


def print_summary( summary ):
    global pp
    for bmark, fdict in summary.iteritems():
        print "[%s]:" % bmark
        for key, value in fdict.iteritems():
            if key == "by_size":
                continue
            if key == "types" or key == "sbysize":
                print "    [%s]: %s" % (key, pp.pformat(value))
            else:
                print "    [%s]: %d" % (key, value)

def main_process( directory = None,
                  logger = None ):
    global pp
    olddir = os.getcwd()
    worklist = [ "_201_compress",
                 "_202_jess",
                 "_205_raytrace",
                 "_209_db",
                 "_213_javac",
                 "_222_mpegaudio",
                 "_227_mtrt",
                 "_228_jack",
                 "avrora",
                 "batik",
                 "fop",
                 "luindex",
                 "lusearch",
                 "specjbb",
                 "tomcat",
                 "xalan", ]
    # Add summary filename or create from function
    summary = {}
    count = 0
    for bmark in worklist:
        summary_path = os.path.join( "./SUMMARY", bmark + "-cpp-SUMMARY.csv" )
        if not os.path.isfile(summary_path):
            logger.critical("[ %s ] - SUMMARY: No such file: %s" % (bmark, str(summary_path)))
            print "[ %s ] - SUMMARY: No such file: %s" % (bmark, str(summary_path))
            continue
        print "=======[ %s ]=========================================================" % bmark
        logger.critical( "=======[ %s ]=========================================================" 
                         % bmark )
        #----------------------------------------------------------------------
        #      SUMMARY
        #----------------------------------------------------------------------
        # Get summary
        sreader = SummaryReader( summary_file = summary_path,
                                 logger = logger )
        sreader.read_summary_file()
        #     get summary by size
        number_of_objects = sreader.get_number_of_objects()
        died_by_stack_after_heap_size = sreader.get_size_died_by_stack_after_heap()
        died_by_stack_only_size = sreader.get_size_died_by_stack_only()
        died_by_stack_size = sreader.get_size_died_by_stack()
        died_by_heap_size = sreader.get_size_died_by_heap()
        died_at_end_size = sreader.get_size_died_at_end()
        size_allocated = sreader.get_final_garbology_alloc_time()
        # TODO: number_of_edges = sreader.get_number_of_edges()
        # TODO: died_by_stack = sreader.get_number_died_by_stack()
        # TODO: died_by_heap = sreader.get_number_died_by_heap()
        # TODO: died_at_end = sreader.get_number_died_at_end()
        # TODO: died_by_stack_after_heap = sreader.get_number_died_by_stack_after_heap()
        # TODO: died_by_stack_only = sreader.get_number_died_by_stack_only()
        max_live_size = sreader.get_max_live_size()
        final_time = sreader.get_final_garbology_time()
        if died_by_stack_size != (died_by_stack_after_heap_size + died_by_stack_only_size):
            print "[ %s ] - size (in bytes) mismatch:"
            print "   stack (total)    = %d" % died_by_stack_size
            print "   stack after heap = %d" % died_by_stack_after_heap_size
            print "   stack only       = %d" % died_by_stack_only_size
            print " ---- redoing stack."
            died_by_stack_size = died_by_stack_only_size + died_by_stack_after_heap_size
        summary[bmark] = { "died_by_stack_size" : died_by_stack_size, # size, not object count
                           "died_by_heap_size" : died_by_heap_size, # size, not object count
                           "died_at_end_size" : died_at_end_size, # size, not object count
                           "died_by_stack_after_heap_size" : died_by_stack_after_heap_size, # size of
                           "died_by_stack_only_size" : died_by_stack_only_size, # size of
                           "max_live_size" : max_live_size,
                           "number_of_objects" : number_of_objects,
                           "size_allocated" : size_allocated, # total allocated in bytes
                           "interesting_size" : size_allocated - died_at_end_size # Filtering out died at end
                           }
        print ">>>> =[%s START]===============================================" % bmark
        pp.pprint(summary[bmark])
        print ">>>> =[%s END]=================================================" % bmark
        print "--------------------------------------------------------------------------------"
        count += 1
        continue
        # DEBUG: if count >= 1:
        # DEBUG:     break
    print "======================================================================"
    print "===========[ SUMMARY ]================================================"
    output_path = directory
    output_path_ALL = os.path.join( output_path, "died_by_summary.csv" )
    output_summary( output_path_ALL = output_path_ALL,
                    summary = summary )
    old_dir = os.getcwd()
    # run ismm2017-plot.R
    render_graphs( rscript_path = "/data/rveroy/bin/Rscript",
                   barplot_script = "ismm2017-plot.R",
                   csvfile = output_path_ALL, # csvfile is the input from the output_summary earlier 
                   graph_dir = output_path,
                   logger = logger )
    print "DEBUG END."
    exit(100)
    #=====[ DONE ]=============================================================
    os.chdir( olddir )
    # Print out results in this format:
    print_summary( summary )
    # TODO: Save the largest X cycles.
    #       This should be done in the loop so to cut down on duplicate work.
    # TODO
    # print "===========[ TYPES ]=================================================="
    # benchmarks = summary.keys()
    # print "---------------[ Common to ALL ]--------------------------------------"
    # common_all = set.intersection( *[ set(summary[b]["types"].keys()) for b in benchmarks ] )
    # common_all = [ rev_typedict[x] for x in common_all ]
    # pp.pprint( common_all )
    # print "---------------[ Counter over all benchmarks ]------------------------"
    # g_types = Counter()
    # for bmark, bdict in summary.iteritems():
    #     g_types.update( bdict["types"] )
    # for key, value in g_types.iteritems():
    #     print "%s: %d" % (rev_typedict[key], value)
    # print "Number of types - global: %d" % len(g_types)
    print "===========[ DONE ]==================================================="
    exit(0)

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "directory", help = "Target output directory" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "run_ismm2017.log" )
    return parser
    # TODO: debugflag = False,
    # TODO: parser.add_argument( "--debug",
    # TODO:                      dest = "debugflag",
    # TODO:                      help = "Enable debug output.",
    # TODO:                      action = "store_true" )
    # TODO: parser.add_argument( "--no-debug",
    # TODO:                      dest = "debugflag",
    # TODO:                      help = "Disable debug output.",
    # TODO:                      action = "store_false" )


def main():
    parser = create_parser()
    args = parser.parse_args()
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = True )
    #
    # Main processing
    #
    return main_process( directory = args.directory,
                         logger = logger )

if __name__ == "__main__":
    main()


#================================================================================
# Old code from output_summary
#
#     print "Summary output path: %s" % str(output_path)
#     # The latest summary
#     with open(output_path, "wb") as fp:
#         csvwriter = csv.writer(fp)
#         bmarklist = summary.keys()
#         # TODO: Multiple sorts of benchmark name?
#         #      - alphabetical is easiest to start with
#         #      - allocation size (largest first)
#         bmarklist = sorted( bmarklist, reverse = True )
#         header = [ "attribute", ]
#         header.extend( bmarklist )
#         csvwriter.writerow( header )
#         attributes = [ "number_of_objects", "size_allocated",
#                        "died_at_end_size", "interesting_size",
#                        "died_by_stack_size", "died_by_heap_size",
#                        "died_by_stack_after_heap_size", "died_by_stack_only_size",
#                        "max_live_size", ]
#         attrs_index = { attributes[x] : x for x in xrange(len(attributes)) }
#         for attr in attributes:
#             row = []
#             row.append( attr )
#             for bmark in bmarklist:
#                 row.append( summary[bmark][attr] )
#             csvwriter.writerow( row )
#             print "XXX:", row
# 
