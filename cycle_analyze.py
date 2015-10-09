# cycle-analyze.py 
#
import argparse
import os
import sys
import time
# from sys import getsizeof
import logging
import sqorm
import cPickle
import pprint
# import exceptions
# from traceback import print_stack
import re
import ConfigParser
from operator import itemgetter
from collections import Counter
import networkx as nx

import mypytools

pp = pprint.PrettyPrinter( indent = 4 )

__MY_VERSION__ = 5

GB = 1099511627776 # 1 gigabyte

my_alloc_types = [ "A", "N", "I", "P", ]

def setup_logger( targetdir = ".",
                  filename = "cycle-analyze.log",
                  logger_name = 'cycle-analyze',
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

def create_record( objId = None,
                   mytype = None,
                   rectype = None ):
    return { "objId" : objId,
             "type" : mytype,
             "rectype" : rectype }

#
# Heap Data structure
# TODO: Do we still need this return tuple?
# hash: objId -> tuple
#    tuple is:
#        if A: (threadId, type, size)
#           D: NONE
#           U: (threadId, newTgtId, oldTgtId)
def populate_newdict_record( rec = None,
                             time_by_method = None,
                             logger = None ):
    global pp

    if rec != None:
        newdict = {}
        if rec["rectype"] in my_alloc_types:
            newdict["at"] = int(time_by_method) # allocation time
            newdict["dt"] = None # death time
            newdict["t"] = rec["type"] # object type
            # Not sure if I need these:
            newdict["f"] = [] # fields
            newdict["atype"] = rec["rectype"] # allocation type
            return newdict
        else:
            logger.error( "invalid rec type: %s  -(expecting an A or N)" % rec["rectype"] )
            raise RuntimeError()
    else:
        logger.error( "Invalid record. %s" % pp.pformat(rec) )
        raise ValueError( "Invalid record." )
    return None

def populate_new_edgedict_record( rec = None,
                                  time_by_method = None,
                                  logger = None ):
    if rec != None:
        return newdict
    else:
        logger.error( "Invalid record. %s" % pp.pformat(rec) )
        raise ValueError( "Invalid record." )
    return None

def update_fields( objrec = None,
                   newId = None ):
    assert( newId != None )
    objrec["f"].append( newId )

def remove_from_fields( objrec = None,
                        tgtId = None ):
    try:
        if tgtId in objrec["f"]:
            objrec["f"].remove( tgtId )
    except:
        print "DEBUG: ", objrec

def create_graph( cycle_pair_list = None,
                  edges = None,
                  logger = None ):
    logger.debug( "Creating graph..." )
    g = nx.DiGraph()
    nodeset = set([])
    for node, mytype in cycle_pair_list:
        g.add_node( n = node,
                    type = mytype )
        nodeset.add(node)
    for edge in edges:
        src = edge[0]
        tgt = edge[1]
        if src in nodeset and tgt in nodeset:
            g.add_edge( src, tgt )
        else:
            if src not in nodeset:
                logger.error("MISSING source node: %s" % str(src))
            if tgt not in nodeset:
                logger.error("MISSING target node: %s" % str(tgt))
    logger.debug( "....done." )
    return g

class ObjDB:
    def __init__( self,
                  objdb1 = None,
                  objdb2 = None,
                  objdb_all = None,
                  debugflag = False,
                  logger = None ):
        self.objdb1 = objdb1
        self.objdb2 = objdb2
        self.objdb_all = objdb_all
        self.sqodb_all = None
        self.alldb = False
        self.sqObj1 = None
        self.sqObj2 = None
        self.logger = logger
        assert( os.path.isfile( objdb_all ) or
                (os.path.isfile( objdb1 ) and os.path.isfile( objdb2 )) )
        if os.path.isfile( objdb_all ):
            try:
                self.sqodb_all = sqorm.Sqorm( tgtpath = objdb_all,
                                              table = "objects",
                                              keyfield = "objId" )
                self.alldb = True
                print "ALLDB"
                return
            except:
                logger.error( "Unable to load DB ALL file %s" % str(objdb) )
                print "Unable to load DB ALL file %s" % str(objdb)
        if os.path.isfile( objdb1 ):
            try:
                self.sqObj1 = sqorm.Sqorm( tgtpath = objdb1,
                                            table = "objects",
                                            keyfield = "objId" )
            except:
                logger.error( "Unable to load DB 1 file %s" % str(objdb) )
                print "Unable to load DB 1 file %s" % str(objdb)
                assert( False )
        assert(self.sqObj1 != None)
        if os.path.isfile( objdb2 ):
            try:
                self.sqObj2 = sqorm.Sqorm( tgtpath = objdb2,
                                           table = "objects",
                                           keyfield = "objId" )
            except:
                logger.error( "Unable to load DB 2 file %s" % str(objdb) )
                print "Unable to load DB 2 file %s" % str(objdb)
                assert( False )

    def get_type( self, objId ):
        db_oType = None
        if self.alldb:
            assert(False)
            try:
                obj = self.sqodb_all[objId]
                db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
            except:
                # self.logger.error( "Objid [ %s ] not found." % str(objId) )
                return None
        else:
            if objId in self.sqObj1:
                try:
                    obj = self.sqObj1[objId]
                    db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
                except:
                    self.logger.error( "Objid [ %s ] not found in DB1." % str(objId) )
            if objId in self.sqObj2:
                try:
                    obj = self.sqObj1[objId]
                    db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
                except:
                    self.logger.error( "Objid [ %s ] not found in DB2." % str(objId) )
                    print "Objid [ %s ] not found in DB2." % str(objId)
                    return None
        return db_oType

def main_process( tgtpath = None,
                  output = None,
                  objdb1 = None,
                  objdb2 = None,
                  objdb_all = None,
                  benchmark = None,
                  debugflag = False,
                  logger = None ):
    global pp
    objdb = ObjDB( objdb1 = objdb1,
                   objdb2 = objdb2,
                   objdb_all = objdb_all,
                   debugflag = debugflag,
                   logger = logger )
    with open(tgtpath) as fp:
        print "FILE %s OPENED." % tgtpath
        start = False
        cycles = []
        for line in fp:
            line = line.rstrip()
            if line.find("----------") == 0:
                start = True if not start else False
                if start:
                    continue
                else:
                    break
            if start:
                line = line.rstrip(",")
                row = line.split(",")
                # print line
                row = [ int(x) for x in row ]
                cycles.append(row)
        start = False
        edges = []
        for line in fp:
            line = line.rstrip()
            if line.find("==========") == 0:
                start = True if not start else False
                print line
                if start:
                    continue
                else:
                    break
            if start:
                # line = line.replace(" -> ", ",")
                row = [ int(x) for x in line.split(" -> ") ]
                # print line
                edges.append(row)
                print row
    print "===========[ CYCLES ]================================================="
    pp.pprint(cycles)
    print "===========[ EDGES ]=================================================="
    edges = sorted( edges, key = itemgetter(0, 1) )
    pp.pprint(edges)
    print "===========[ TYPES ]=================================================="
    typedict = {}
    group = 1
    graphs = []
    for cycle in cycles:
        typelist = []
        print "==========[ Group %d ]=================================================" % group
        cycle_pair_list = []
        for node in cycle:
            mytype = objdb.get_type(node)
            mytype = mytype if mytype != None else "NONE"
            cycle_pair_list.append( (node, mytype) )
            if mytype:
                typelist.append(mytype)
                if node in typedict:
                    if typedict[node] != mytype:
                        print "ObjId[ %d ] has conflicting types: %s --- %s" % (typedict[node], mytype)
                        logger.error( "ObjId[ %d ] has conflicting types: %s --- %s" % (typedict[node], mytype) )
                else:
                    typedict[node] = mytype
        # Create the graph
        graph_name = "%s-%d.dot" % (benchmark, group)
        G = create_graph( cycle_pair_list = cycle_pair_list,
                          edges = edges,
                          logger = logger )
        nx.write_dot(G, graph_name)
        # Get the actual cycle
        real_cycle = list(nx.simple_cycles(G))
        # Print out typelist
        pp.pprint(Counter(typelist))
        # Print the cycle
        print "CYCLE by networkx:"
        pp.pprint(real_cycle)
        group += 1
    print "===========[ GLOBAL TYPE DICTIONARY ]================================="
    pp.pprint(typedict)
    print "===========[ DONE ]==================================================="
    exit(1000)

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
    print "CONFIG."
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( args.config )
    config = config_section_map( "global", config_parser )
    pp.pprint(config)
    return config

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "filename", help = "Source file from simulator run." )
    parser.add_argument( "--config",
                         help = "Specify configuration filename.",
                         action = "store" )
    parser.add_argument( "--benchmark",
                         required = True,
                         help = "Set name of benchmark" )
    parser.add_argument( "--outpickle",
                         required = True,
                         help = "Target output pickle filename." )
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
    parser.set_defaults( logfile = "cycle-analyze.log",
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
    logfile = "cycle-analyze-" + os.path.basename(tgtpath) + ".log" if not logfile else logfile    

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
    print "CONFIG."
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( args.config )
    global_config = config_section_map( "global", config_parser )
    objdb1_config = config_section_map( "objdb1", config_parser )
    objdb2_config = config_section_map( "objdb2", config_parser )
    objdb_ALL_config = config_section_map( "objdb_ALL", config_parser )
    print "GLOBAL:"
    pp.pprint(global_config)
    print "OBJDB:"
    pp.pprint(objdb1_config)
    pp.pprint(objdb2_config)
    pp.pprint(objdb_ALL_config)
    return ( global_config, objdb1_config, objdb2_config, objdb_ALL_config )

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    benchmark = args.benchmark
    print "Benchmark", benchmark
    if args.config != None:
         global_config, objdb1_config, objdb2_config, objdb_ALL_config = process_config( args )
    else:
        # TODO
        assert( False )
        TODO_ = process_args( args, parser )
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )

    # set up objdb
    objdb1 = os.path.join( global_config["objdb_dir"], objdb1_config[benchmark] )
    objdb2 = os.path.join( global_config["objdb_dir"], objdb2_config[benchmark] )
    objdb_all = os.path.join( global_config["objdb_dir"], objdb_ALL_config[benchmark] )
    print "XXX", objdb_all
    print "   ", objdb1
    print "   ", objdb2
    #
    # Main processing
    #
    return main_process( tgtpath = args.filename,
                         output = args.outpickle,
                         debugflag = global_config["debug"],
                         objdb1 = objdb1,
                         objdb2 = objdb2,
                         objdb_all = objdb_all,
                         benchmark = benchmark,
                         logger = logger )

if __name__ == "__main__":
    main()
