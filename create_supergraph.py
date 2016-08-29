from __future__ import division
# create_supergraph.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
from collections import Counter
from collections import defaultdict
import networkx as nx

# Possible useful libraries, classes and functions:
# from operator import itemgetter
#   - This one is my own library:
# from mypytools import mean, stdev, variance
from mypytools import create_work_directory

# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import ObjectInfoReader, StabilityReader, ReferenceReader, \
         ReverseRefReader, get_index, is_stable

# Needed to read in *-OBJECTINFO.txt and other files from 
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time


pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "create_supergraph.log",
                  logger_name = 'create_supergraph',
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
def is_array( mytype ):
    return (len(mytype) > 0) and (mytype[0] == "[")

def is_primitive_type( mytype = None ):
    return ( (mytype == "Z")     # boolean
              or (mytype == "B") # byte
              or (mytype == "C") # char
              or (mytype == "D") # double
              or (mytype == "F") # float
              or (mytype == "I") # int
              or (mytype == "J") # long
              or (mytype == "S") # short
              )

def is_primitive_array( mytype = None ):
    # Is it an array?
    if not is_array(mytype):
        return False
    else:
        return ( is_primitive_type(mytype[1:]) or
                 is_primitive_array(mytype[1:]) )
        
# TODO: Refactor out
def check_host( benchmark = None,
                worklist_config = {},
                host_config = {} ):
    thishost = socket.gethostname()
    for wanthost in worklist_config[benchmark]:
        if thishost in host_config[wanthost]:
            return True
    return False

# TODO: Refactor out
def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None

def create_supergraph_all( datadict = {} ):
    # Get all the objects and add as a node to the graph
    result = {}
    TYPE = get_index( "TYPE" ) # type index
    for bmark, mydict in datadict.iteritems():
        dgraph = nx.DiGraph()
        objreader = mydict["objreader"]
        for tup in objreader.iterrecs():
            objId, rec = tup
            mytype = objreader.get_type_using_typeId( rec[TYPE] )
            dgraph.add_node( objId, { "type" : mytype } )
        # Add the stable edges only
        stability = mydict["stability"]
        reference = mydict["reference"]
        for objId, fdict in stability.iteritems():
            for fieldId, sattr in fdict.iteritems():
                if is_stable(sattr):
                    # Add the edge
                    try:
                        objlist = reference[ (objId, fieldId) ]
                    except:
                        print "ERROR: Not found (%s, %s)" % (str(objId), str(fieldId))
                        logger.error("ERROR: Not found (%s, %s)" % (str(objId), str(fieldId)))
                        print "EXITING."
                        exit(10)
                    for tgtId in objlist:
                        dgraph.add_edge( objId, tgtId )
        # Save the directed graph in the result dictionary
        result[bmark] = dgraph
    return result

def main_process( output = None,
                  global_config = {},
                  objectinfo_config = {},
                  worklist_config = {},
                  main_config = {},
                  reference_config = {},
                  reverse_ref_config = {},
                  stability_config = {},
                  debugflag = False,
                  logger = None ):
    global pp
    # This is where the simulator output files are
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Get the date and time to label the work directory.
    today = date.today()
    today = today.strftime("%Y-%m%d")
    timenow = datetime.now().time().strftime("%H-%M-%S")
    olddir = os.getcwd()
    # TODO delete old debug: print main_config["output"]
    os.chdir( main_config["output"] )
    workdir = create_work_directory( work_dir = main_config["output"],
                                     today = today,
                                     timenow = timenow,
                                     logger = logger,
                                     interactive = False )
    # Take benchmarks to process from create-supergraph-worklist 
    #     in basic_merge_summary configuration file.
    # Where to get file?
    # Filenames are in
    #   - objectinfo_config, reference_config, reverse_ref_config, stability_config
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    datadict = { bmark : {} for bmark in worklist_config.keys() }
    for bmark in datadict.keys():
        datadict[bmark]["objreader"] = ObjectInfoReader( os.path.join( cycle_cpp_dir,
                                                                       objectinfo_config[bmark] ),
                                                         logger = logger )
        datadict[bmark]["stability"] = StabilityReader( os.path.join( cycle_cpp_dir,
                                                                      stability_config[bmark] ),
                                                        logger = logger )
        datadict[bmark]["reference"] = ReferenceReader( os.path.join( cycle_cpp_dir,
                                                                      reference_config[bmark] ),
                                                        logger = logger )
        datadict[bmark]["reverse-ref"] = ReverseRefReader( os.path.join( cycle_cpp_dir,
                                                                         reverse_ref_config[bmark] ),
                                                           logger = logger )
    for bmark, mydict in datadict.iteritems():
        # Read in OBJECTINFO
        print "Reading in the OBJECTINFO file for benchmark:", bmark
        objreader = mydict["objreader"]
        objreader.read_objinfo_file()
        # Read in STABILITY
        print "Reading in the STABILITY file for benchmark:", bmark
        stabreader = mydict["stability"]
        stabreader.read_stability_file()
        # Read in REFERENCE
        print "Reading in the REFERENCE file for benchmark:", bmark
        refreader = mydict["reference"]
        refreader.read_reference_file()
        # Read in REVERSE-REFERENCE
        print "Reading in the REVERSE-REFERENCE file for benchmark:", bmark
        reversereader = mydict["reverse-ref"]
        reversereader.read_reverseref_file()
    print "DONE reading all benchmarks."
    print "================================================================================"
    print "Creating the supergraph..."
    supergraph = create_supergraph_all( datadict = datadict )
    for bmark, graph in supergraph.iteritems():
        wcclist = sorted( nx.weakly_connected_component_subgraphs(graph),
                          key = len,
                          reverse = True )
        print "[%s] -> # of objects = %d" % (bmark, len(objreader))
        print "     -> nodes = %d  edges = %d  - WCC = %d" % \
            ( graph.number_of_nodes(),
              graph.number_of_edges(),
              len(wcclist) )
    print "================================================================================"
    print "create_supergraph.py - DONE."
    os.chdir( olddir )
    exit(0)

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
    main_config = config_section_map( "summarize-objectinfo", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    worklist_config = config_section_map( "summarize-objectinfo-worklist", config_parser )
    reference_config = config_section_map( "reference", config_parser )
    reverse_ref_config = config_section_map( "reverse-reference", config_parser )
    stability_config = config_section_map( "stability-summary", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    # PROBABLY NOT: summary_config = config_section_map( "summary_cpp", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "worklist" : worklist_config,
             "reference" : reference_config,
             "reverse-reference" : reverse_ref_config,
             "stability" : stability_config,
             # "summary" : summary_config,
             # "contextcount" : contextcount_config,
             # "host" : host_config,
             }

def process_host_config( host_config = {} ):
    for bmark in list(host_config.keys()):
        hostlist = host_config[bmark].split(",")
        host_config[bmark] = hostlist
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
    parser.add_argument( "output", help = "Target output filename." )
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
    parser.set_defaults( logfile = "create_supergraph.log",
                         debugflag = False,
                         config = None )
    return parser

def calculate_counts( datadict = None ):
    # TODO At end, and global result dictionaries?
    result = {}
    DIEDBY = get_index( "DIEDBY" ) # died by index
    ATTR = get_index( "STATTR" ) # stack attribute index
    TYPE = get_index( "TYPE" ) # type index
    for bmark, mydict in datadict.iteritems():
        objreader = mydict["objreader"]
        result[bmark] = {}
        rtmp = result[bmark]
        rtmp["stack_after_heap"] = Counter()
        rtmp["heap"] = Counter()
        rtmp["stack_only"] = Counter()
        rtmp["end_of_prog"] = Counter()
        rtmp["others"] = Counter()
        # TODO rtmp["stack_all"] = Counter()
        for tup in objreader.iterrecs():
            # TODO: Refactor this
            objId, rec = tup
            reason = rec[DIEDBY]
            stack_attr = rec[ATTR]
            mytype = objreader.get_type_using_typeId( rec[TYPE] )
            if reason == "S":
                # TODO: rtmp["stack_all"][mytype] += 1
                if stack_attr == "SHEAP":
                    rtmp["stack_after_heap"][mytype] += 1
                elif stack_attr == "SONLY":
                    rtmp["stack_only"][mytype] += 1
            elif reason == "H":
                rtmp["heap"][mytype] += 1
            elif reason == "E":
                rtmp["end_of_prog"][mytype] += 1
            else:
                rtmp["others"][mytype] += 1
    return result

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    assert( args.config != None )
    # Get the configurations we need.
    configdict = process_config( args )
    global_config = configdict["global"]
    main_config = configdict["main"]
    objectinfo_config = configdict["objectinfo"]
    reference_config = configdict["reference"]
    reverse_ref_config = configdict["reverse-reference"]
    stability_config = configdict["stability"]
    worklist_config = process_worklist_config( configdict["worklist"] )
    # pp.pprint(worklist_config)
    # PROBABLY DELETE:
    # contextcount_config = configdict["contextcount"]
    # host_config = process_host_config( configdict["host"] )
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         global_config = global_config,
                         main_config = main_config,
                         objectinfo_config = objectinfo_config,
                         worklist_config = worklist_config,
                         reference_config = reference_config,
                         reverse_ref_config = reverse_ref_config,
                         stability_config = stability_config,
                         logger = logger )

if __name__ == "__main__":
    main()
