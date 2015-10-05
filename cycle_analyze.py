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

# TODO TODO TODO TODO
def update_heap_record( heap = None,
                        rec = None,
                        recObjId = None,
                        time_by_method = None,
                        logger = None ):
    global pp
    if rec != None:
        objId = rec["objId"]
        oldTgtId = rec["oldTgtId"]
        newTgtId = rec["newTgtId"]
        # TODO: Debug code that could be removed to improve performance
        if objId not in heap:
            raise RuntimeError( "objId[ %s ] not found in heap." % objId )
        if newTgtId not in heap:
            logger.warning( "newTgtId[ %s ] not found in heap." % newTgtId )
            # logger.error( " record:\n    %s" % pp.pformat(rec) )
            newrec = create_record( objId = newTgtId,
                                    rectype = "A" )
            newdict = populate_newdict_record( rec = newrec,
                                               time_by_method = time_by_method,
                                               logger = logger )
            if newdict != None:
                objdict[newTgtId] = newdict
        # Update "fields"
        remove_from_fields( objrec = heap[objId],
                            tgtId = oldTgtId )
        update_fields( objrec = heap[objId],
                       newId = newTgtId )
        # TODO: Call update_fields function here.
    else:
        logger.error( "invalid record." )
        raise exceptions.ValueError( "rec can not be 'None'" )
    return
# TODO TODO TODO TODO

def create_graph( heap = None ):
    global logger, pp
    logger.debug( "Creating graph..." )
    g = nx.DiGraph()
    for obj, val in heap.iteritems():
        otype = str(val["type"]) if "type" in val \
                else "None"
        osize = str(val["size"]) if "size" in val \
                else "None"
        g.add_node( n = obj,
                    type = otype,
                    size = osize )
    for obj, rec in heap.iteritems():
        try:
            assert( "f" in rec )
        except:
            logger.error( "No fields in rec: %s" % pp.pformat(rec) )
        else:
            for tgt in rec["f"]:
                if tgt != '0':
                    g.add_edge( obj, tgt )
    logger.debug( "....done." )
    return g

def pickle_all( objdict = None,
                objfilename = None,
                edgedict = None,
                edgefilename = None,
                logger = None ):
    global pp
    logger.debug( "Attempting to pickle to [%s]:", objfilename )
    print "Attempting to pickle to [%s]:", objfilename
    try:
        objfile = open( objfilename, 'wb' )
    except:
        logger.error( "Unable to open objdict pickle file: %s", objfilename )
        exit(41)
    if objfile != None:
        cPickle.dump( objdict, objfile )
    print "======================================================================"
    logger.debug( "Attempting to pickle to [%s]:", edgefilename )
    print "Attempting to pickle to [%s]:", edgefilename
    try:
        edgefile = open( edgefilename, 'wb' )
    except:
        logger.error( "Unable to open edgedict pickle file: %s", edgefilename )
        exit(41)
    if edgefile != None:
        cPickle.dump( edgedict, edgefile )

def process_input( conn = None,
                   version = None,
                   stopline = None,
                   objdict = None,
                   edgedict = None,
                   waitdict = None,
                   logger = None ):
    # TODO: time_by_method needs to be adjusted from the multiprocessing
    deadhash = {}
    ignored_alloc = set([])
    strace = STrace( Stats = Stats )
    cur = 0
    time_by_method = 0
    skip_count = 0
    gbcount = 1
    for x in conn:
        rec = parse_line( line = x,
                          version = version,
                          logger = logger )
        cur = cur + 1
        time_by_method = process_heap_event( version = version,
                                             objdict = objdict,
                                             edgedict = edgedict,
                                             waitdict = waitdict,
                                             deadhash = deadhash,
                                             ignored_alloc = ignored_alloc,
                                             rec = rec,
                                             strace = strace,
                                             time_by_method = time_by_method,
                                             logger = logger )
        if stopline > 0 and stopline == cur:
            break
    assert("TIME" not in objdict)
    objdict["TIME"] = time_by_method
    print "DONE size[ %d ]." % len(objdict)

def main_process( tgtpath = None,
                  objdb = None,
                  debugflag = False,
                  logger = None ):
    global pp
    with open(tgtpath) as fp:
        print "FILE %s OPENED." % tgtpath
        data = cPickle.load(fp)
    pp.pprint(data)
    try:
        sqobj = sqorm.Sqorm( tgtpath = objdb,
                             table = "objects",
                             keyfield = "objId" )
    except:
        logger.critical( "Unable to load DB file %s" % str(objdb) )
        print "Unable to load DB file %s" % str(objdb)
        exit(2)
    found = {}
    miss = set([])
    one_cycle_list = []
    cycles_all = []
    for cycle in data:
        oneflag = True if len(cycle) == 1 else False
        cycle_types = []
        for objId in cycle:
            try:
                obj = sqobj[objId]
                db_objId, db_oType, db_oSize, db_oLen, db_oAtime, db_oDtime, db_oSite = obj
                sys.stdout.write(".")
                found[objId] = (db_oType, db_oSize)
                cycle_types.append( db_oType )
                if oneflag:
                    one_cycle_list.append( db_oType )
            except:
                logger.error( "No object with id: %d" % objId )
                sys.stdout.write("X")
                miss.update( [ objId ] )
        cycles_all.append( cycle_types )
    # print "================================================================================"
    # print "==========[ FOUND ]============================================================="
    # pp.pprint( found )
    # print "================================================================================"
    # print "==========[ MISSING ]==========================================================="
    # print "MISSING:"
    # pp.pprint( miss )
    print "================================================================================"
    print "==========[ TYPELIST ]=========================================================="
    typelist = [ val[0] for key, val in found.iteritems() ]
    pp.pprint( typelist )
    print "================================================================================"
    print "==========[ TYPE COUNTS ]======================================================="
    counter = Counter( typelist )
    pp.pprint( dict(counter) )
    print "================================================================================"
    print "==========[ ONE CYCLES ]========================================================"
    pp.pprint( set(one_cycle_list) )
    print "================================================================================"
    print "==========[ ALL CYCLES ]========================================================"
    for cycle in cycles_all:
        print "----------------------------------------"
        print dict(Counter(cycle))
        print "Total: %d" % len(cycle)
    print "----------------------------------------"
    print "==========[ done ]=============================================================="
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
                         action = "store",
                         default = None )
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
                         debugflag = False )
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
    objdb_config = config_section_map( "objdb", config_parser )
    print "GLOBAL:"
    pp.pprint(global_config)
    print "OBJDB:"
    pp.pprint(objdb_config)
    return ( global_config, objdb_config )

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()

    if args.config != None:
        config, objdb_config = process_config( args )
    else:
        # TODO
        assert( False )
        TODO_ = process_args( args, parser )

    # set up objdb
    objdb = os.path.join( config["objdb_dir"], objdb_config[benchmark] )
    print "OBJDB: %s" % objdb
    #
    # Main processing
    #
    return main_process( tgtpath = args.pickle,
                         debugflag = config["debug"],
                         objdb = objdb,
                         logger = logger )

if __name__ == "__main__":
    main()
