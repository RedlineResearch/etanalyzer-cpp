# gcsim_graph.py 
#
import argparse
import ConfigParser
import os
import sys
import time
import logging
import pprint
import re
import csv
import subprocess
import datetime

from shutil import move, rmtree
from glob import glob

# Probably need the statistical methods
from mypytools import mean, stdev, variance, \
    check_host, create_work_directory, process_host_config, \
    process_worklist_config, get_trace_fp
# MAYBE: 
# is_specjvm, is_dacapo, is_minibench, 

# MAYBE TODO from operator import itemgetter
# MAYBE TODO import tarfile
# TODO import cPickle
# TODO import sqorm
# TODO from collections import Counter
# TODO import networkx as nx
# TODO import StringIO
# TODO import heapq
# TODO from itertools import combinations


pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "gcsim_graph.log",
                  logger_name = 'gcsim_graph',
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

#
# Main processing
#


# TODO def row_to_string( row ):
# TODO     result = None
# TODO     strout = StringIO.StringIO()
# TODO     csvwriter = csv.writer(strout)
# TODO     # Is the list comprehension necessary? Doesn't seem like it.
# TODO     csvwriter.writerow( [ x for x in row ] )
# TODO     result = strout.getvalue()
# TODO     strout.close()
# TODO     return result.replace("\r", "")

def render_graphs( rscript_path = None,
                   barplot_script = None,
                   csvfile = None,
                   graph_dir = None,
                   logger = None,
                   debugflag = False ):
    curdir = os.getcwd()
    csvfile_abs = os.path.join( curdir, csvfile )
    assert( os.path.isfile( rscript_path ) )
    assert( os.path.isfile( barplot_script ) )
    print csvfile
    print csvfile_abs
    assert( os.path.isfile( csvfile_abs ) )
    assert( os.path.isdir( graph_dir ) )
    cmd = [ rscript_path, # The Rscript executable
            barplot_script, # Our R script that generates the plots/graphs
            csvfile_abs, # The csv file that contains the data
            graph_dir, ] # Where to place the PDF output files
    print "Running R barplot script  on %s -> directory %s" % (csvfile, graph_dir)
    logger.debug( "[ %s ]" % str(cmd) )
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    if debugflag:
        logger.debug("--------------------------------------------------------------------------------")
        for x in result:
            logger.debug(str(x))
        logger.debug("--------------------------------------------------------------------------------")
    # TODO: Parse the result to figure out if it went wrong.

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

def backup_old_graphs( graph_dir_path = None,
                       backup_graph_dir_path = None,
                       base_temp_dir = None,
                       pdfs_config = None,
                       today = None ):
    assert( os.path.isdir( backup_graph_dir_path ) )
    assert( os.path.isdir( graph_dir_path ) )
    temp_dir = mkdtemp( dir = base_temp_dir )
    print "Using temporary directory: %s" % temp_dir
    # tar and bzip2 -9 all old files to today.tar
    # Move all files to TEMP_DIR
    tfilename_base = os.path.join( backup_graph_dir_path, "object_barplots-" + today + "*" )
    flist = glob( tfilename_base )
    i = len(flist) + 1
    tempbase = "object_barplots-%s-%s.tar" % (today, str(i))
    tfilename = os.path.join( temp_dir, tempbase )
    assert( not os.path.exists( tfilename ) )
    tarfp = tarfile.open( tfilename, mode = 'a' )
    print "Taring to: %s" % tfilename
    os.chdir( graph_dir_path )
    flist = []
    for fname in os.listdir( graph_dir_path ):
        if ( skip_file( fname ) or
             os.path.samefile( tfilename, fname ) ):
            continue
        flist.append( fname )
    if len(flist) == 0:
        print "Empty source graph directory: %s" % graph_dir_path
        print " - attempting to remove %s" % temp_dir
        rmtree( temp_dir )
        return
    for fname in flist:
        if os.path.isfile( fname ):
            print "  adding %s..." % fname
            tarfp.add( fname )
    tarfp.close()
    for fname in flist:
        if os.path.isfile( fname ):
            print "  deleting %s." % fname
            os.remove( fname )
    cmd = [ "bzip2", "-9", tfilename ]
    print "Bzipping...",
    proc = subprocess.Popen( cmd,
                             stdout = subprocess.PIPE,
                             stderr = subprocess.PIPE )
    result = proc.communicate()
    # TODO Check result?
    print "DONE."
    print os.listdir( temp_dir )
    bz2filename = tfilename + ".bz2"
    print "--------------------------------------------------------------------------------"
    assert( os.path.isfile(bz2filename) )
    # Check to see if the target exists already.
    tgtfile = os.path.join( backup_graph_dir_path, bz2filename )
    print "Moving: %s --> %s" % (bz2filename, backup_graph_dir_path)
    move( bz2filename, backup_graph_dir_path )
    print "Attempting to remove %s" % temp_dir
    rmtree( temp_dir )

def main_process( main_config = None,
                  global_config = None,
                  debugflag = False,
                  logger = None ):
    global pp
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    graph_dir_path = global_config["graph_dir"]
    backup_graph_dir_path = global_config["backup_graph_dir"]
    temp_dir = global_config["temp_dir"]
    work_dir = main_config["directory"]
    results = {}
    # Get the date and time to label the work directory.
    today = date.today()
    today = today.strftime("%Y-%m%d")
    timenow = datetime.now().time().strftime("%H-%M-%S")
    olddir = os.getcwd()
    os.chdir( main_config["output"] )
    workdir = create_work_directory( work_dir,
                                     today = today,
                                     timenow = timenow,
                                     logger = logger,
                                     interactive = False )
    for bmark, filename in etanalyze_config.iteritems():
        hostlist = worklist_config[bmark]
        if not check_host( benchmark = bmark,
                           hostlist = hostlist,
                           host_config = host_config ):
            continue
        # Else we can run for 'bmark'
        # TODO TODO TODO cachesize = int(cachesize_config[bmark])
        if mprflag:
            print "=======[ Spawning %s ]================================================" \
                % bmark
            results[bmark] = manager.list([ bmark, ])
            # NOTE: The order of the args tuple is important!
            # ======================================================================
            # Read in the CYCLES (death groups file from simulator) 
            p = Process( target = read_dgroups_into_pickle,
                         args = ( results[bmark],
                                  bmark,
                                  workdir,
                                  mprflag,
                                  dgroups_config,
                                  cycle_cpp_dir,
                                  objectinfo_db_config,
                                  cachesize,
                                  debugflag,
                                  logger ) )
            procs_dgroup[bmark] = p
            p.start()
        else:
            print "=======[ Running %s ]=================================================" \
                % bmark
            print "     Reading in cycles (death groups)..."
            results[bmark] = [ bmark, ]
            read_dgroups_into_pickle( result = results[bmark],
                                      bmark = bmark,
                                      workdir = workdir,
                                      mprflag = mprflag,
                                      dgroups_config = dgroups_config,
                                      cycle_cpp_dir = cycle_cpp_dir,
                                      objectinfo_db_config = objectinfo_db_config,
                                      obj_cachesize = cachesize,
                                      debugflag = debugflag,
                                      logger = logger )
    if mprflag:
        # Poll the processes 
        done = False
        while not done:
            done = True
            for bmark in procs_dgroup.keys():
                proc = procs_dgroup[bmark]
                proc.join(60)
                if proc.is_alive():
                    done = False
                else:
                    del procs_dgroup[bmark]
                    timenow = time.asctime()
                    logger.debug( "[%s] - done at %s" % (bmark, timenow) )
        print "======[ Processes DONE ]========================================================"
        sys.stdout.flush()
    print "================================================================================"
    # Copy all the databases into MAIN directory.
    dest = main_config["output"]
    for filename in os.listdir( workdir ):
        # Check to see first if the destination exists:
        # print "XXX: %s -> %s" % (filename, filename.split())
        # Split the absolute filename into a path and file pair:
        # Use the same filename added to the destination path
        tgtfile = os.path.join( dest, filename )
        if os.path.isfile(tgtfile):
            try:
                os.remove(tgtfile)
            except:
                logger.error( "Weird error: found the file [%s] but can't remove it. The copy might fail." % tgtfile )
        print "Copying %s -> %s." % (filename, dest)
        copy( filename, dest )
    print "================================================================================"
    print "dgroups2db.py - DONE."
    os.chdir( olddir )
    exit(0)
    print "======================================================================"
    print "===========[ SUMMARY ]================================================"
    output_summary( output_path = output,
                    summary = summary )
    old_dir = os.getcwd()
    backup_old_graphs( graph_dir_path = graph_dir_path,
                       pdfs_config = pdfs_config,
                       backup_graph_dir_path = backup_graph_dir_path,
                       base_temp_dir = temp_dir,
                       today = today )
    os.chdir( old_dir )
    # run object_barplot.R
    render_graphs( rscript_path = global_config["rscript_path"],
                   barplot_script = global_config["barplot_script"],
                   csvfile = output, # csvfile is the input from the output_summary earlier 
                   graph_dir = global_config["graph_dir"],
                   logger = logger,
                   debugflag = debugflag )
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
    parser.set_defaults( logfile = "gcsim_graph.log",
                         debugflag = False,
                         lastedgeflag = False,
                         benchmark = "_ALL_",
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
    main_config = config_section_map( "gcsim-graph", config_parser )
    run_gcsim_config = config_section_map( "run-GCsim", config_parser )
    # TODO summary_config = config_section_map( "summary-cpp", config_parser )
    # TODO etanalyze_config = config_section_map( "etanalyze-output", config_parser )
    # TODO objectinfo_config = config_section_map( "objectinfo", config_parser )
    return { "global" :  global_config,
             "main" : main_config,
             "run_gcsim_config" : run_gcsim_config,
             }

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    benchmark = args.benchmark
    assert( args.config != None )
    config_result = process_config( args )
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         main_config = main_config,
                         global_config = global_config,
                         logger = logger )

if __name__ == "__main__":
    main()
