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
import shutil
from multiprocessing import Process, Queue

# Possible useful libraries, classes and functions:
# from operator import itemgetter
#   - This one is my own library:
# from mypytools import mean, stdev, variance

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

def output_graph_and_summary( bmark = "",
                              objreader = {},
                              dgraph = {},
                              wcclist = [],
                              backupdir = None,
                              logger = None ):
    # Print to standard output
    print "[%s] -> # of objects = %d" % (bmark, len(objreader))
    print "     -> nodes = %d  edges = %d  - WCC = %d" % \
        ( dgraph.number_of_nodes(),
          dgraph.number_of_edges(),
          len(wcclist) )
    print "     -> 3 largest WCC = %d, %d, %d" % \
        ( len(wcclist[0]), len(wcclist[1]), len(wcclist[2]) )
    target = "%s-stable_graph.gml" % bmark
    # Backup the old gml file if it exists
    if os.path.isfile(target):
        # Move this file into backup directory
        bakfile = os.path.join( backupdir, target )
        if os.path.isfile( bakfile ):
            os.remove( bakfile )
        shutil.move( target, backupdir )
    nx.write_gml(dgraph, target)
        

def read_simulator_data( bmark = "",
                         cycle_cpp_dir = "",
                         objectinfo_config = {},
                         stability_config = {},
                         reference_config = {},
                         reverse_ref_config = {},
                         mydict = {},
                         logger = None ):
    # Read in OBJECTINFO
    print "Reading in the OBJECTINFO file for benchmark:", bmark
    mydict["objreader"] = ObjectInfoReader( os.path.join( cycle_cpp_dir,
                                                          objectinfo_config[bmark] ),
                                            logger = logger )
    objreader = mydict["objreader"]
    try:
        objreader.read_objinfo_file()
    except:
        logger.error( "[ %s ] Unable to read objinfo file.." % bmark )
        mydict.clear()
        sys.stdout.flush()
        return False
    # Read in STABILITY
    print "Reading in the STABILITY file for benchmark:", bmark
    mydict["stability"] = StabilityReader( os.path.join( cycle_cpp_dir,
                                                         stability_config[bmark] ),
                                           logger = logger )
    try:
        stabreader = mydict["stability"]
        stabreader.read_stability_file()
    except:
        logger.error( "[ %s ] Unable to read stability file.." % bmark )
        mydict.clear()
        sys.stdout.flush()
        return False
    # Read in REFERENCE
    print "Reading in the REFERENCE file for benchmark:", bmark
    mydict["reference"] = ReferenceReader( os.path.join( cycle_cpp_dir,
                                                         reference_config[bmark] ),
                                           logger = logger )
    try:
        refreader = mydict["reference"]
        refreader.read_reference_file()
    except:
        logger.error( "[ %s ] Unable to read reference file.." % bmark )
        mydict.clear()
        sys.stdout.flush()
        return False
    # Read in REVERSE-REFERENCE
    print "Reading in the REVERSE-REFERENCE file for benchmark:", bmark
    mydict["reverse-ref"] = ReverseRefReader( os.path.join( cycle_cpp_dir,
                                                            reverse_ref_config[bmark] ),
                                              logger = logger )
    try:
        reversereader = mydict["reverse-ref"]
        reversereader.read_reverseref_file()
    except:
        logger.error( "[ %s ] Unable to read reverse-reference file.." % bmark )
        mydict.clear()
        sys.stdout.flush()
        return False
    sys.stdout.flush()
    return True


def create_supergraph_all( bmark = "",
                           cycle_cpp_dir = {},
                           main_config = {},
                           objectinfo_config = {},
                           stability_config = {},
                           reference_config = {},
                           reverse_ref_config = {},
                           logger = None ):
    # Assumes that we are in the desired working directory.
    # Get all the objects and add as a node to the graph
    mydict = {}
    backupdir = main_config["backup"]
    read_result = read_simulator_data( bmark = bmark,
                                       cycle_cpp_dir = cycle_cpp_dir,
                                       objectinfo_config = objectinfo_config,
                                       stability_config = stability_config,
                                       reference_config = reference_config,
                                       reverse_ref_config = reverse_ref_config,
                                       mydict = mydict,
                                       logger = logger )
    if read_result == False:
        return False
    TYPE = get_index( "TYPE" ) # type index
    print "======[ %s ]====================================================================" % bmark
    dgraph = nx.DiGraph()
    objreader = mydict["objreader"]
    objnode_list =  set([])
    for tup in objreader.iterrecs():
        objId, rec = tup
        mytype = objreader.get_type_using_typeId( rec[TYPE] )
        if objId not in objnode_list:
            dgraph.add_node( objId, { "type" : mytype } )
            objnode_list.add( objId )
        else:
            logger.critical( "Multiple add for object Id [ %s ]" % str(objId) )
    # Add the stable edges only
    stability = mydict["stability"]
    reference = mydict["reference"]
    for objId, fdict in stability.iteritems(): # for each object
        for fieldId, sattr in fdict.iteritems(): # for each field Id of each object
            if is_stable(sattr):
                # Add the edge
                try:
                    objlist = reference[ (objId, fieldId) ]
                except:
                    print "ERROR: Not found (%s, %s)" % (str(objId), str(fieldId))
                    logger.error("ERROR: Not found (%s, %s)" % (str(objId), str(fieldId)))
                    print "EXITING."
                    exit(10)
                if objId != 0:
                    if objId not in objnode_list:
                        print "=========[ ERROR ]=============================================================="
                        pp.pprint( objnode_list )
                        print "=========[ ERROR ]=============================================================="
                        print "ObjId [ %s ] of type [ %s ]" % (str(objId), str(type(objId)))
                        assert(False)
                        # continue # TODO TODO TODO
                else:
                    continue
                missing = set([])
                for tgtId in objlist: # for each target object
                    if tgtId != 0:
                        if tgtId in missing:
                            continue
                        elif tgtId not in objnode_list:
                            missing.add( tgtId )
                            print "=========[ ERROR ]=============================================================="
                            print "Missing objId [ %s ] of type [ %s ]" % (str(tgtId), str(type(tgtId)))
                            continue # For now. TODO TODO TODO
                        dgraph.add_edge( objId, tgtId )
    wcclist = sorted( nx.weakly_connected_component_subgraphs(dgraph),
                      key = len,
                      reverse = True )
    output_graph_and_summary( bmark = bmark,
                              objreader = objreader,
                              dgraph = dgraph,
                              wcclist = wcclist,
                              backupdir = backupdir,
                              logger = logger )
    print "------[ %s DONE ]---------------------------------------------------------------" % bmark

def create_supergraph_all_MPR( bmark = "",
                               cycle_cpp_dir = "",
                               main_config = {},
                               objectinfo_config = {},
                               stability_config = {},
                               reference_config = {},
                               reverse_ref_config = {},
                               logger = None ):
    # Assumes that we are in the desired working directory.
    # Get all the objects and add as a node to the graph
    mydict = {}
    backupdir = main_config["backup"]
    read_result = read_simulator_data( bmark = bmark,
                                       cycle_cpp_dir = cycle_cpp_dir,
                                       objectinfo_config = objectinfo_config,
                                       stability_config = stability_config,
                                       reference_config = reference_config,
                                       reverse_ref_config = reverse_ref_config,
                                       mydict = mydict,
                                       logger = logger )
    if read_result == False:
        return False
    TYPE = get_index( "TYPE" ) # type index
    dgraph = nx.DiGraph()
    objreader = mydict["objreader"]
    objnode_list =  set([])
    for tup in objreader.iterrecs():
        objId, rec = tup
        mytype = objreader.get_type_using_typeId( rec[TYPE] )
        if objId not in objnode_list:
            dgraph.add_node( objId, { "type" : mytype } )
            objnode_list.add( objId )
        else:
            logger.critical( "%s: Multiple add for object Id [ %s ]" %
                             (bmark, str(objId)) )
    # Add the stable edges only
    stability = mydict["stability"]
    reference = mydict["reference"]
    for objId, fdict in stability.iteritems(): # for each object
        for fieldId, sattr in fdict.iteritems(): # for each field Id of each object
            if is_stable(sattr):
                # Add the edge
                try:
                    objlist = reference[ (objId, fieldId) ]
                except:
                    print "ERROR: Not found (%s, %s)" % (str(objId), str(fieldId))
                    logger.error("ERROR: Not found (%s, %s)" % (str(objId), str(fieldId)))
                    print "EXITING."
                    exit(10)
                if objId != 0:
                    if objId not in objnode_list:
                        print "=========[ ERROR ]=============================================================="
                        pp.pprint( objnode_list )
                        print "=========[ ERROR ]=============================================================="
                        print "ObjId [ %s ] of type [ %s ]" % (str(objId), str(type(objId)))
                        assert(False)
                        # continue # TODO TODO TODO
                else:
                    continue
                missing = set([])
                for tgtId in objlist: # for each target object
                    if tgtId != 0:
                        if tgtId in missing:
                            continue
                        elif tgtId not in objnode_list:
                            missing.add( tgtId )
                            print "=========[ ERROR ]=============================================================="
                            print "Missing objId [ %s ] of type [ %s ]" % (str(tgtId), str(type(tgtId)))
                            continue # For now. TODO TODO TODO
                        dgraph.add_edge( objId, tgtId )
    wcclist = sorted( nx.weakly_connected_component_subgraphs(dgraph),
                      key = len,
                      reverse = True )
    output_graph_and_summary( bmark = bmark,
                              objreader = objreader,
                              dgraph = dgraph,
                              wcclist = wcclist,
                              backupdir = backupdir,
                              logger = logger )
    return { "graph" : dgraph, "wcclist" : wcclist }

def main_process( global_config = {},
                  objectinfo_config = {},
                  worklist_config = {},
                  main_config = {},
                  reference_config = {},
                  reverse_ref_config = {},
                  stability_config = {},
                  mprflag = False,
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
    workdir =  main_config["output"]
    os.chdir( workdir )
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
    supergraph = {}
    for bmark in datadict.keys():
        # TODO START
        procs = {}
        if mprflag:
            print "=======[ Spawning %s ]================================================" \
                % bmark
            p = Process( target = create_supergraph_all_MPR,
                         args = ( bmark,
                                  cycle_cpp_dir,
                                  main_config,
                                  objectinfo_config,
                                  stability_config,
                                  reference_config,
                                  reverse_ref_config,
                                  logger ) )
            p.start()
            procs[bmark] = p
        else:
            create_supergraph_all( bmark = bmark,
                                   cycle_cpp_dir = cycle_cpp_dir,
                                   main_config = main_config,
                                   objectinfo_config = objectinfo_config,
                                   stability_config = stability_config,
                                   reference_config = reference_config,
                                   reverse_ref_config = reverse_ref_config,
                                   logger = logger )
        # TODO END
        print "[%s]" % str(bmark)
        # TODO HERE TODO
    if mprflag:
        # Poll the processes 
        done = False
        while not done:
            done = True
            for bmark in procs.keys():
                proc = procs[bmark]
                proc.join(60)
                if proc.is_alive():
                    done = False
                else:
                    del procs[bmark]
        print "======[ Processes DONE ]========================================================"
    # TODO for bmark, graph in supergraph.iteritems():
    # TODO     wcclist = sorted( nx.weakly_connected_component_subgraphs(graph),
    # TODO                       key = len,
    # TODO                       reverse = True )
    # TODO     print "[%s] -> # of objects = %d" % (bmark, len(objreader))
    # TODO     print "     -> nodes = %d  edges = %d  - WCC = %d" % \
    # TODO         ( graph.number_of_nodes(),
    # TODO           graph.number_of_edges(),
    # TODO           len(wcclist) )
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
    main_config = config_section_map( "create-supergraph", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    worklist_config = config_section_map( "create-supergraph-worklist", config_parser )
    reference_config = config_section_map( "reference", config_parser )
    reverse_ref_config = config_section_map( "reverse-reference", config_parser )
    stability_config = config_section_map( "stability-summary", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    # PROBABLY NOT: summary_config = config_section_map( "summary_cpp", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "create-supergraph-worklist" : worklist_config,
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
    parser.add_argument( "--config",
                         help = "Specify configuration filename.",
                         action = "store" )
    parser.add_argument( "--mpr",
                         dest = "mprflag",
                         help = "Enable multiprocessing.",
                         action = "store_true" )
    parser.add_argument( "--single",
                         dest = "mprflag",
                         help = "Single threaded operation.",
                         action = "store_false" )
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
                         mprflag = False,
                         debugflag = False,
                         config = None )
    return parser

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
    worklist_config = process_worklist_config( configdict["create-supergraph-worklist"] )
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
    config_debugflag = global_config["debug"]
    return main_process( debugflag = (config_debugflag if config_debugflag else args.debugflag),
                         mprflag = args.mprflag,
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
