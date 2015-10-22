# basic_merge_summary.py 
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
import ConfigParser
from operator import itemgetter
from collections import Counter
from itertools import chain, repeat
import csv
import networkx as nx

import mypytools

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

def setup_logger( targetdir = ".",
                  filename = "basic_merge_summary.log",
                  logger_name = 'basic_merge_summary',
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

def create_edge_dictionary( edges = None ):
    edgedict = {}
    for edge in edges:
        src = edge[0]
        tgt = edge[1]
        if src not in edgedict:
            edgedict[src] = [ tgt ]
        else:
            edgedict[src].append(tgt)
    for src, tgtlist in edgedict.iteritems():
        edgedict[src] = set(tgtlist)
    return edgedict

def create_graph( cycle_pair_list = None,
                  edgedict = None,
                  logger = None ):
    global pp
    logger.debug( "Creating graph..." )
    g = nx.DiGraph()
    nodeset = set([])
    for mytuple in cycle_pair_list:
        node, mytype, mysize = mytuple
        nodeset.add(node)
        g.add_node( n = node,
                    type = mytype )
        if node in edgedict:
            for tgt in edgedict[node]:
                g.add_edge( node, tgt )
    logger.debug( "....done." )
    return g

def get_filelist(tgtpath):
    # We assume that the summary filename has the benchmark name 
    # as follows:
    filere = re.compile("(.*)-summary.txt$")
    dirlist = os.listdir(tgtpath)
    filedict = {}
    for item in dirlist:
        m = filere.match(item)
        if m:
            benchmark = m.group(1)
            assert(benchmark not in filedict)
            actual_path = os.path.join(tgtpath, item)
            filedict[benchmark] = actual_path
    return filedict

def expand_counter(counter):
    count_list = sorted( [ (k, v) for (k, v) in counter.iteritems() ],
                         key = itemgetter(0) )
    return chain( *[ list(z) for z in [ repeat(x, y) for (x, y) in count_list ] ] )

def main_process( tgtpath = None,
                  debugflag = False,
                  logger = None ):
    global pp
    filedict = get_filelist(tgtpath)
    bmark_re = re.compile("^benchmark: (.*)$")
    num_re = re.compile("^num_cycles: (.*)$")
    cycle_total_re  = re.compile("^cycle_total_counter: (.*)$")
    result = {}
    for benchmark, filename in filedict.iteritems():
        with open(filename) as fp:
            for line in fp:
                m = bmark_re.match(line)
                if m:
                    bmark_input = m.group(1)
                    assert(bmark_input == benchmark)
                    result[bmark_input] = {}
                    continue
                m = num_re.match(line)
                if m:
                    num_cycles = int(m.group(1))
                    result[bmark_input]["num_cycles"] = num_cycles
                    continue
                m = cycle_total_re.match(line)
                if m:
                    cycle_counter = m.group(1)
                    result[bmark_input]["cycle_counter"] = eval(cycle_counter)
                    continue
                continue
    csvlist = []
    for benchmark, mydict in result.iteritems():
        for key, val in mydict.iteritems():
            if key == "num_cycles":
                pass # TODO TODO TODO TODO
            elif key == "cycle_counter":
                citer = expand_counter(val)
            else:
                assert(False)
        for x in citer:
            print "%s,%d" % (benchmark, x)
    exit(1000) # TODO TODO TODO

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

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "directory", help = "Source directory where all the summaries are." )
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
    parser.set_defaults( logfile = "basic_merge_summary.log",
                         debugflag = False,
                         benchmark = False,
                         config = None )
    return parser

def process_args( args, parser ):
    #
    # Get input filename
    #
    tgtpath = args.pickle
    try:
        if not os.path.exists( tgtpath ):
            parser.error( tgtpath + " does not exist." )
    except:
        parser.error( "invalid path name : " + tgtpath )
    # Actually open the input db/file in main_process()
    # 
    # Get logfile
    logfile = args.logfile
    logfile = "basic_merge_summary-" + os.path.basename(tgtpath) + ".log" if not logfile else logfile    

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
    global pp
    assert( args.config != None )
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( args.config )
    global_config = config_section_map( "global", config_parser )
    return global_config

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    if args.config != None:
         config = process_config( args )
    else:
        # TODO
        assert( False )
        TODO_ = process_args( args, parser )
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = config["debug"] )
    #
    # Main processing
    #
    return main_process( tgtpath = args.directory,
                         debugflag = config["debug"],
                         logger = logger )

if __name__ == "__main__":
    main()
