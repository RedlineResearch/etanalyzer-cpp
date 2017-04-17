# run_GC_simulator.py 
#
from __future__ import division
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
from operator import itemgetter
from collections import Counter, defaultdict
import csv
import datetime

import subprocess
# TODO from twisted.internet import protocol, reactor

from mypytools import mean, stdev, variance

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

def setup_logger( targetdir = ".",
                  filename = "run_GC_simulator.log",
                  logger_name = 'run_GC_simulator',
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

def render_histogram( histfile = None,
                      title = None ):
    outpng = histfile + ".png"
    cmd = [ "/data/rveroy/bin/Rscript",
            "/data/rveroy/pulsrc/etanalyzer/Rgraph/histogram.R", # TODO Hard coded for now.
            # Put into config. TODO TODO TODO
            histfile, outpng,
            "800", "800",
            title, ]
    print "Running histogram.R on %s -> %s" % (histfile, outpng)
    print "[ %s ]" % cmd
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    print "--------------------------------------------------------------------------------"
    for x in result:
        print x
    print "--------------------------------------------------------------------------------"

def create_work_directory( work_dir, logger = None, interactive = False ):
    os.chdir( work_dir )
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    work_today = "simGC-" + today
    if os.path.isfile(work_today):
        print "Can not create %s as directory." % work_today
        exit(11)
    if not os.path.isdir( work_today ):
        os.mkdir( work_today )
    else:
        print "WARNING: %s directory exists." % work_today
        logger.warning( "WARNING: %s directory exists." % work_today )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    return work_today

def check_subprocesses( procdict = {},
                        done_proclist = None,
                        finish_all_flag = False ):
    done = False
    num = 0
    count = 0
    while not done:
        done = False
        for sproc in procdict.keys():
            if count % 20 == 1:
                sys.stdout.write( "Checking process %s for size %d:"
                                  % (str(sproc), procdict[sproc]) )
            retcode = sproc.poll()
            if retcode != None:
                del procdict[sproc]
                done_proclist.append(sproc)
                done = True
                num += 1
        if finish_all_flag:
            done = (len(procdict.keys()) == 0)
    return num

def get_output( done_proclist = [],
                results = {} ):
    for sproc in done_proclist:
        rdict = results[sproc]
        while True:
            line = sproc.stdout.readline()
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
            else:
                break

def main_process( simulator = None,
                  benchmark = None,
                  tracefile = None,
                  namesfile = None,
                  dgroupsfile = None,
                  output = None,
                  minheap = 0,
                  maxheap = 0,
                  step = 4194304, # 4 MB default
                  numprocs = 1,
                  debugflag = False,
                  logger = None ):
    assert( benchmark != None )
    assert( os.path.isfile( simulator ) )
    assert( os.path.isfile( tracefile ) )
    assert( os.path.isfile( namesfile ) )
    assert( os.path.isfile( dgroupsfile ) )
    assert( output != None )
    assert( minheap >= 0 )
    assert( type(maxheap) is int )
    # Note: if maxheap = 0, then go until there's no GC.
    assert( type(numprocs) is int )
    assert( logger != None )
    # TODO:
    procdict = {}
    done_proclist = []
    heapsize_dict = {}
    # Option 1:
    # Start from maxlivesize * 1.05
    # Using the given increment, loop through until no GC. or until maxheap
    #
    # Option 2:
    # Do a binary search. This works best when we start with max live size as starting point
    # and say 5 to 7 times max live size? Or maybe just total allocation * 90% or something?
    # start with min
    end_heapsize = maxheap if (maxheap > 0) else (20 * minheap)
    print "minheap:", minheap
    print "end_heapsize:", end_heapsize
    print "step:", step
    results = defaultdict( lambda: { "total_objects" : 0,
                                     "total_alloc" : 0,
                                     "number_collections" : 0,
                                     "mark_total" : 0,
                                     "mark_saved" : 0, } )
    for heapsize in range(minheap, end_heapsize, step):
        # Running the 'simulator-GC' needs the following args:
        #  simulator-GC _201_compress.names 0-CURRENT/_201_compress-DGROUPS.csv _201_compress 7918525
        cmd = [ simulator, namesfile, dgroupsfile, benchmark, str(heapsize) ]
        print cmd
        fp = get_trace_fp( tracefile, logger )
        sproc = subprocess.Popen( cmd,
                                  stdout = subprocess.PIPE,
                                  stdin = fp,
                                  stderr = subprocess.PIPE )
        procdict[sproc] = heapsize
        heapsize_dict[sproc] = heapsize
        # The output of the process we are interested in:
        #---------------------------------------------------------------------
        # Done at time 144677956
        # Total objects: 5230301
        # Total allocated in bytes:     362596760
        # Number of collections: 1151
        # Mark total   : 3341799675
        # - mark saved : 14063174
        # - total alloc: 362596760
        #---------------------------------------------------------------------
        rdict = results[sproc]
        if len(procdict.keys()) < numprocs:
            continue
        else:
            num_done = check_subprocesses( procdict = procdict,
                                           done_proclist = done_proclist,
                                           finish_all_flag = False )
            get_output( done_proclist = done_proclist,
                        results = results )
    num_done = check_subprocesses( procdict = procdict,
                                   done_proclist = done_proclist,
                                   finish_all_flag = True )
    with open( outpu, "wb" ) as fptr:
        writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
        writer.writerow( [ "benchmark", "heapsize", "total_alloc",
                           "number_collections", "mark_total", "mark_saved", ] )
        for sproc, rdict in results.items():
            heapsize = heapsize_dict[sproc]
            row = [ benchmark, heapsize, rdict["total_alloc"], rdict["number_collections"],
                    rdict["mark_total"], rdict["mark_saved"], ]
            writer.writerow( row )
    print "DONE."
    exit(0)

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "output", help = "Target output filename." )
    parser.add_argument( "--simulator",
                         dest = "simulator",
                         help = "GC simulator executable path.",
                         action = "store" )
    parser.add_argument( "--tracefile",
                         dest = "tracefile",
                         help = "Source ET event trace file.",
                         action = "store" )
    parser.add_argument( "--namesfile",
                         dest = "namesfile",
                         help = "Associated ET names file.",
                         action = "store" )
    parser.add_argument( "--dgroupsfile",
                         dest = "dgroupsfile",
                         help = "Death groups file from dgroups2db.py.",
                         action = "store" )
    parser.add_argument( "--minheapsize",
                         dest = "minheapsize",
                         help = "Starting heap size.",
                         action = "store",
                         type = int )
    parser.add_argument( "--numprocs",
                         dest = "numprocs",
                         help = "Number of processes at a time.",
                         action = "store",
                         type = int )
    parser.add_argument( "--debug",
                         dest = "debugflag",
                         help = "Enable debug output.",
                         action = "store_true" )
    parser.add_argument( "--no-debug",
                         dest = "debugflag",
                         help = "Disable debug output.",
                         action = "store_false" )
    parser.add_argument( "--benchmark",
                         dest = "benchmark",
                         help = "Select benchmark.",
                         action = "store" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "run_GC_simulator.log",
                         debugflag = False,
                         numprocs = 1,
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

def process_global_config( gconfig ):
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( gconfig )
    global_config = config_section_map( "global", config_parser )
    return global_config

def process_runsim_config( runconfig ):
    runconfig_parser = ConfigParser.ConfigParser()
    runconfig_parser.read( runconfig )
    run_global_config = config_section_map( "global", runconfig_parser )
    run_benchmarks = config_section_map( "benchmarks", runconfig_parser )
    return ( run_global_config, run_benchmarks )

def main():
    global pp

    parser = create_parser()
    args = parser.parse_args()
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = args.debugflag )
    #
    # Main processing
    #
    return main_process( simulator = args.simulator,
                         benchmark = args.benchmark,
                         minheap = args.minheapsize,
                         tracefile = args.tracefile,
                         namesfile = args.namesfile,
                         dgroupsfile = args.dgroupsfile,
                         output = args.output,
                         numprocs = args.numprocs,
                         debugflag = args.debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()
