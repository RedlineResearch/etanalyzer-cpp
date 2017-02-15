from __future__ import division
# analyze_dgroup.py
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
from multiprocessing import Process, Manager, Queue
import sqlite3
from shutil import copy, move
import cPickle
from itertools import chain
import networkx as nx
from operator import itemgetter
from collections import Counter
from collections import defaultdict
#   - This one is my own library:
from mypytools import mean, stdev, variance, generalised_sum
from mypytools import check_host, create_work_directory, process_host_config, \
    process_worklist_config, dfs_iter, remove_dupes, bytes_to_MB

# TODO: Do we need 'sqorm.py' or 'sqlitetools.py'?
#       Both maybe? TODO

# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import ObjectInfoReader, get_edgeinfo_db_filename, EdgeInfoReader, \
    SummaryReader
#     ObjectInfoFile2DB, EdgeInfoFile2DB, StabilityReader

# Needed to read in *-OBJECTINFO.txt and other files from
# the simulator run
import csv

# For timestamping directories and files.
from datetime import datetime, date
import time

pp = pprint.PrettyPrinter( indent = 4 )

KEY_OBJECT_SUMMARY = "key-object-summary.csv"

def setup_logger( targetdir = ".",
                  filename = "analyze_dgroup.py.log",
                  logger_name = 'analyze_dgroup.py',
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

def check_diedby_stats( dgroups_data = {},
                        objectinfo = {} ):
    result = defaultdict( dict )
    tmp = 0
    for gnum, glist in dgroups_data["group2list"].iteritems():
        result[gnum]["diedby"] = { "count" : Counter(),
                                   "types" : defaultdict( set ) }
        result[gnum]["deathtime"] = Counter()
        for objId in glist:
            cause = objectinfo.get_death_cause(objId)
            # last_actual_ts = objectinfo.get_last_actual_timestamp(objId)
            dtime = objectinfo.get_death_time(objId)
            result[gnum]["diedby"]["count"][cause] += 1
            mytype = objectinfo.get_type(objId)
            result[gnum]["diedby"]["types"][cause].add(mytype)
            result[gnum]["deathtime"][dtime] += 1
        # DEBUG
        tmp += 1
        if tmp >= 20:
            break
    return result

def update_age_summaries( dsite = None,
                          nonjdsite = None,
                          glist = [],
                          dsites_age = {},
                          nonjlib_age = {},
                          objectinfo = {} ):
    # Rename/alias into shorter names
    oi = objectinfo
    age_list = [ oi.get_age_ALLOC(objId) for objId in glist ]
    age_list = filter( lambda x: x != 0,
                       age_list )
    if len(age_list) == 0:
        return
    new_min = min(age_list)
    new_max = max(age_list)
    for agedict, mydsite in [ (dsites_age, dsite), (nonjlib_age, nonjdsite) ]:
        count, total = agedict[mydsite]["gensum"]
        # TODO DEBUG ONLY print "XXX", count, total
        new_age_list = [ total ]
        new_age_list.extend( age_list )
        _tmp, new_total = generalised_sum( new_age_list, None )
        # _tmp is ignored
        # Save the new count of objects.
        #   - 1     : is needed because the total is counted as one BUT
        #   + count : ... is actually count objects long already
        new_count = len(new_age_list) - 1 + count
        agedict[mydsite]["gensum"] = (new_count, new_total)
        agedict[mydsite]["min"] = \
            min( new_min, agedict[mydsite]["min"] ) if (agedict[mydsite]["min"] != 0) \
            else new_min
        agedict[mydsite]["max"] = max( new_max, agedict[mydsite]["max"] )

def get_group_died_by_attribute( group = set(),
                                 objectinfo = {} ):
    oi = objectinfo
    result = defaultdict( list )
    for objId in group:
        cause = objectinfo.get_death_cause(objId)
        result[cause].append(objId)
    # Check here. There should only be one cause.
    assert(len(result) > 0)
    if len(result) == 1:
        # As expected. The following works because keys() should
        # have EXACTLY one key.
        cause = result.keys()[0]
    else:
        # While this shouldn't happen, this is how we fix the errors.
        if "E" in result.keys():
            # First if any are Died by End, everything should be.
            # TODO: This should be fixed in the simulator. If not there,
            #       then in csvinfo2db.py. - RLV
            cause = "E"
        elif "S" in result.keys():
            cause = "S"
        else:
            # More than one Died By XXX. DEBUG.
            print "=====[ DEBUG MULTIPLE CAUSES ]=================================================="
            for cause, objlist in result.iteritems():
                print "------------------------------------------------------------"
                print "Cause[ %s ]:" % cause
                for objId in objlist:
                    rec = oi.get_record(objId)
                    print "    [%d] : %s - %d" % ( objId,
                                                   oi.get_type_using_record(rec),
                                                   oi.get_death_time_using_record(rec) )
                print "------------------------------------------------------------"
            # If that didn't work, let's break the tie democratically.
            clist = sorted( [ (c, len(olist)) for c, olist in result.items() ],
                            key = itemgetter(1),
                            reverse = True )
            print "Sorted cause list:"
            pp.pprint(clist)
            print "================================================================================"
            cause = clist[0][0]
            if (cause == "G" or cause == "H"):
                cause = "H"
            else:
                raise RuntimeError("DEBUG: %s" % str(clist))

    return "STACK" if cause == "S" else \
        ( "HEAP" if (cause == "H" or cause == "G") else
          ( "END" if cause == "E" else None ) )

# GLOBAL:
#     summary dictionary layout:
#        key - summary name of table. Possible keys are:
#            * "CAUSE"
#               + distribution among by heap/stack/progend
#            * "TYPES"
#               + type distribution for the key objects
#            * "DSITES"
#               + death site/context distribution for the key objects
# BY TYPE:
#     summary_by_type dictionary layout:
#        key - key object type
#        value - dictionary of attributes
#            subkeys:
#                * number of key objects of this type
#                * group sizes
#                * group types (simple set seems the most logical)
#                * death site counts
#                * allocation site counts
#                * count of how many escaped? TODO
def update_key_object_summary( newgroup = {},
                               summary = {},
                               objectinfo = {},
                               total_size = 0,
                               logger = None ):
    # Rename/alias long names:
    oi = objectinfo
    cdict = summary["CAUSE"]
    dsitesdict = summary["DSITES"]
    dsites_size = summary["DSITES_SIZE"]
    dsites_gcount = summary["DSITES_GROUP_COUNT"]
    dsites_distr = summary["DSITES_DISTRIBUTION"]
    dsites_age = summary["DSITES_AGE"]
    nonjlib_dsitesdict = summary["NONJLIB_DSITES"]
    nonjlib_dsites_size = summary["NONJLIB_SIZE"]
    nonjlib_gcount = summary["NONJLIB_GROUP_COUNT"]
    nonjlib_distr = summary["NONJLIB_DISTRIBUTION"]
    nonjlib_age = summary["NONJLIB_AGE"]
    typedict = summary["TYPES"]
    type_dsites = summary["TYPE_DSITES"]
    # TODO: nonjlib_type_dsites = summary["NONJLIB_TYPE_DSITES"]
    # Remember the total size of the group in bytes
    # ------------------------------------------------------------
    # Part 1 - Get the by heap/stack/etc stats for key objects only
    #     Note that this means we're counting groups instead of objects.
    for key, glist in newgroup.iteritems():
        if oi.died_by_stack(key):
            cdict["STACK"] += 1
        elif oi.died_by_heap(key) or oi.died_by_global(key):
            cdict["HEAP"] += 1
        elif oi.died_by_program_end(key):
            cdict["PROGEND"] += 1
        else:
            sys.stderr.write( "Object ID[%d] not by S/H/PE. Saving as UNKNOWN.\n" )
            cdict["UNKNOWN"] += 1
    # Part 2 - Get the type and the death distributions
    for key, glist in newgroup.iteritems():
        # Well, there should really be only one group in newgroup...
        summary["TOTAL_SIZE"][key] = total_size
        if key not in glist:
            glist.append(key)
        summary["GROUPLIST"][key] = glist
        mytype = oi.get_type(key)
        typedict[mytype] += 1
        # First level death site
        dsite = oi.get_death_context(key)
        dsitesdict[dsite] += 1
        dsites_size[dsite] += total_size
        type_dsites[type][dsite] += 1
        dsites_gcount[dsite] += 1
        # Non java library death site
        nonjdsite = oi.get_non_javalib_context(key)
        nonjlib_dsitesdict[nonjdsite] += 1
        nonjlib_dsites_size[nonjdsite] += total_size
        nonjlib_gcount[nonjdsite] += 1
        # TODO: nonjlib_type_dsites[type][nonjdsite] += 1
        # Update the distribution summaries
        if ( oi.died_by_heap(key) or
             oi.died_by_stack_after_heap(key) or
             oi.died_by_global(key) ):
            dsites_distr[dsite]["HEAP"] += total_size
            nonjlib_distr[nonjdsite]["HEAP"] += total_size
        elif oi.died_by_stack(key):
            dsites_distr[dsite]["STACK"] += total_size
            nonjlib_distr[nonjdsite]["STACK"] += total_size
        else:
            # Sanity check
            if not ( oi.died_by_program_end(key) or oi.died_at_end(key) ):
                # Debug something that fell outside of all categories
                logger.critical( "Unable to classify objId[ %d ][ %s ]"
                                 % (key, oi.get_type(key)) )
                sys.stderr.write( "Unable to classify objId[ %d ][ %s ]\n"
                                  % (key, oi.get_type(key)) )
        update_age_summaries( dsite = dsite,
                              nonjdsite = nonjdsite,
                              glist = glist,
                              dsites_age = dsites_age,
                              nonjlib_age = nonjlib_age,
                              objectinfo = oi )


def read_dgroups_from_pickle( result = [],
                              bmark = "",
                              workdir = "",
                              mprflag = False,
                              dgroups2db_config = {},
                              objectinfo_db_config = {},
                              summary_config = {},
                              cycle_cpp_dir = "",
                              obj_cachesize = 5000000,
                              debugflag = False,
                              cycle_result = [],
                              logger = None ):
    assert(logger != None)
    # print os.listdir( )
    #===========================================================================
    # Read in OBJECTINFO
    print "Reading in the OBJECTINFO file for benchmark:", bmark
    sys.stdout.flush()
    oread_start = time.clock()
    print " - Using objectinfo DB:"
    db_filename = os.path.join( cycle_cpp_dir,
                                objectinfo_db_config[bmark] )
    objectinfo = ObjectInfoReader( useDB_as_source = True,
                                   db_filename = db_filename,
                                   cachesize = obj_cachesize,
                                   logger = logger )
    objectinfo.read_objinfo_file()
    #===========================================================================
    # Read in EDGEINFO
    print "Reading in the EDGEINFO file for benchmark:", bmark
    sys.stdout.flush()
    oread_start = time.clock()
    print " - Using objectinfo DB:"
    edgedb_filename = get_edgeinfo_db_filename( workdir = cycle_cpp_dir,
                                                bmark = bmark )
    print "EDGEDB:", edgedb_filename
    edgeinfo = EdgeInfoReader( useDB_as_source = True,
                               edgedb_filename = edgedb_filename,
                               cachesize = obj_cachesize,
                               logger = logger )
    # Unlike the ObjectInfoReader for DB files, EdgeInfoReader does all the
    # necessary initialization in __init__. I need to fix this aysmmetry in
    # the API design somehow. Maybe later. TODO TODO - RLV 29 Dec 2016
    #===========================================================================
    # Read in SUMMARY
    print "Reading in the SUMMARY file for benchmark:", bmark
    sys.stdout.flush()
    summary_filename = os.path.join( cycle_cpp_dir,
                                     summary_config[bmark] )
    oread_start = time.clock()
    print "SUMMARY:", summary_filename
    summary_reader = SummaryReader( summary_file = summary_filename,
                                    logger = logger )
    summary_reader.read_summary_file()
    #===========================================================================
    # Read in DGROUPS from the pickle file
    picklefile = os.path.join( dgroups2db_config["output"],
                               bmark + dgroups2db_config["file-dgroups"] )
    assert(os.path.isfile(picklefile))
    with open(picklefile, "rb") as fptr:
        dgroups_data = cPickle.load(fptr)
    #===========================================================================
    # Process
    #
    # TODO: # Idea 1: for each group, check that each object in the group have the same
    # TODO: # died by
    # TODO: diedby_results = check_diedby_stats( dgroups_data = dgroups_data,
    # TODO:                                      objectinfo = objectinfo )
    # TODO: print "==========================================================================="
    # TODO: for gnum, datadict in diedby_results.iteritems():
    # TODO:     assert("diedby" in datadict)
    # TODO:     assert("deathtime" in datadict)
    # TODO:     sys.stdout.write( "GROUP %d\n" % gnum )
    # TODO:     sys.stdout.write( "    * DIEDBY:\n" )
    # TODO:     for diedbytype, total in datadict["diedby"]["count"].iteritems():
    # TODO:         sys.stdout.write( "        %s -> %d\n" % (diedbytype, total) )
    # TODO:         sys.stdout.write( "         - Types: %s\n" % str(list( datadict["diedby"]["types"][diedbytype])) )
    # TODO:     sys.stdout.write( "    * dtime               : %s\n" % str(list(datadict["deathtime"])) )
    # TODO:     sys.stdout.write( "===========================================================================\n" )
    # TODO:     sys.stdout.flush()
    #===========================================================================
    # Idea 2: Get the key objects TODO TODO TODO
    #
    print "--------------------------------------------------------------------------------"
    keytype_counter = Counter()
    deathsite_summary = defaultdict( Counter )
    dss = deathsite_summary # alias
    cycle_summary = Counter() # Keys are type tuples
    count = 0
    key_objects = { "GROUPLIST" : {},
                    "CAUSE" : Counter(),
                    "DSITES" : Counter(),
                    "DSITES_SIZE" : defaultdict(int),
                    "DSITES_GROUP_COUNT" : Counter(),
                    "DSITES_DISTRIBUTION" :
                        defaultdict( lambda: defaultdict(int) ),
                    "DSITES_AGE" :
                        defaultdict( lambda: { "max" : 0, "min" : 0, "ave" : 0, "gensum" : (0, 0) } ),
                    "NONJLIB_DSITES" : Counter(),
                    "NONJLIB_SIZE" : defaultdict(int),
                    "NONJLIB_GROUP_COUNT" : Counter(),
                    "NONJLIB_DISTRIBUTION" :
                        defaultdict( lambda: defaultdict(int) ),
                    "NONJLIB_AGE" :
                        defaultdict( lambda: { "max" : 0, "min" : 0, "ave" : 0, "gensum" : (0, 0) } ),
                    "TYPES" : Counter(),
                    "TYPE_DSITES" : defaultdict(Counter),
                    "TOTAL_SIZE" : {}, }
    seen_objects = set()
    total_alloc = 0
    total_died_at_end_size = 0
    for gnum, glist in dgroups_data["group2list"].iteritems():
        # - for every death group dg:
        #       get the last edge for every object
        key_result, total_size, died_at_end_size, cause = get_key_objects( group = glist,
                                                                           seen_objects = seen_objects,
                                                                           edgeinfo = edgeinfo,
                                                                           objectinfo = objectinfo,
                                                                           cycle_summary = cycle_summary,
                                                                           logger = logger )
        count += 1
        if cause == "END":
            assert(len(key_result) == 0)
            assert( died_at_end_size > 0 )
            total_died_at_end_size += died_at_end_size
        elif len(key_result) > 0:
            assert( (cause == "HEAP") or (cause == "STACK") )
            total_alloc += total_size
            update_key_object_summary( newgroup = key_result,
                                       summary = key_objects,
                                       objectinfo = objectinfo,
                                       total_size = total_size,
                                       logger = logger )
            # TODO DEBUG print "%d: %s" % (count, key_result)
            ktc = keytype_counter # Short alias
            for key, subgroup in key_result.iteritems():
                # Summary of key types
                keytype = objectinfo.get_type(key)
                ktc[keytype] += 1
                groupsize = len(subgroup)
                # Summary of death locations
                deathsite = objectinfo.get_death_context(key)
                dss[keytype][deathsite] += 1
        # TODO DEBUG print "--------------------------------------------------------------------------------"
    # Save the CSV file the key object summary
    total_objects = summary_reader.get_number_of_objects()
    num_key_objects = len(key_objects["GROUPLIST"])
    size_died_at_end = summary_reader.get_size_died_at_end()
    size_total_allocation = summary_reader.get_final_garbology_alloc_time()
    dsites_gcount = key_objects["DSITES_GROUP_COUNT"]
    #-------------------------------------------------------------------------------
    # Alloc age computation TODO TODO TODO
    # Max and min have been maintained for each death site so nothing needed here.
    # Update the averages/mean:
    dsites_age = key_objects["DSITES_AGE"]
    for mydsite in dsites_age.keys():
        count, total = dsites_age[mydsite]["gensum"]
        dsites_age[mydsite]["ave"] = (total / count) if count > 0 \
            else 0
    nonjlib_age = key_objects["NONJLIB_AGE"]
    for mydsite in nonjlib_age.keys():
        count, total = nonjlib_age[mydsite]["gensum"]
        assert( count > 0 )
        nonjlib_age[mydsite]["ave"] = total / count
    #-------------------------------------------------------------------------------
    # First level death sites
    total_alloc_MB = bytes_to_MB(total_alloc)
    actual_alloc = total_alloc - total_died_at_end_size
    actual_alloc_MB = bytes_to_MB(actual_alloc)
    newrow = [ (bmark, total_alloc_MB, actual_alloc_MB) ]
    # Get the top 5 death sites
    #     * Use "DSITES_SIZE" and sort. Get top 5.
    dsites_distr = key_objects["DSITES_DISTRIBUTION"]
    dsites_size = sorted( key_objects["DSITES_SIZE"].items(),
                          key = itemgetter(1),
                          reverse = True )
    newrow = newrow + [ ( x[0], bytes_to_MB(x[1]), ((x[1]/actual_alloc) * 100.0),
                          ((dsites_distr[x[0]]["STACK"]/x[1]) * 100.0),
                          ((dsites_distr[x[0]]["HEAP"]/x[1]) * 100.0),
                          dsites_gcount[x[0]],
                          dsites_age[x[0]]["min"], dsites_age[x[0]]["max"],  dsites_age[x[0]]["ave"], )
                        for x in dsites_size[0:5] ]
    newrow = [ x for tup in newrow for x in tup ]
    #-------------------------------------------------------------------------------
    # First non Java library function
    # TODO TODO: Add allocation total
    newrow_nonjlib = [ ((bmark + " NONJ"), total_alloc_MB, actual_alloc_MB), ]
    # Get the top 5 death sites
    #     * Use "NONJLIB_SIZE" and sort. Get top 5.
    nonjlib_distr = key_objects["NONJLIB_DISTRIBUTION"]
    nonjlib_gcount = key_objects["NONJLIB_GROUP_COUNT"]
    nonjlib_dsites_size = sorted( key_objects["NONJLIB_SIZE"].items(),
                                  key = itemgetter(1),
                                  reverse = True )
    # TODO TODO Add dsite-total, dsite percentage using MB
    dsites_age = key_objects["DSITES_AGE"]
    nonjlib_age = key_objects["NONJLIB_AGE"]
    newrow_nonjlib = newrow_nonjlib + [ ( x[0], bytes_to_MB(x[1]), ((x[1]/actual_alloc) * 100.0),
                                          ((nonjlib_distr[x[0]]["STACK"]/x[1]) * 100.0),
                                          ((nonjlib_distr[x[0]]["HEAP"]/x[1]) * 100.0),
                                          nonjlib_gcount[x[0]],
                                          nonjlib_age[x[0]]["min"], nonjlib_age[x[0]]["max"],  nonjlib_age[x[0]]["ave"], )
                                        for x in nonjlib_dsites_size[0:5] ]
    newrow_nonjlib = [ x for tup in newrow_nonjlib for x in tup ]
    #-------------------------------------------------------------------------------
    # TODO DELETE DEBUG print "X:", newrow
    # Write out the row
    # TODO: REMOVE: key_summary_writer.writerow( newrow )
    # TODO: REMOVE: key_summary_writer.writerow( newrow_nonjlib )
    if not mprflag:
        result.append( newrow )
        result.append( newrow_nonjlib )
        cycle_result.append( cycle_summary )
    else:
        result.put( newrow )
        result.put( newrow_nonjlib )
        cycle_result.put( cycle_summary )
    #
    #       * size stats for groups that died by stack
    #            + first should be number of key objects
    #            + then average size of sub death group
    #===========================================================================
    #
    # pickle_filename = os.path.join( workdir, bmark + "-DGROUPS.pickle" )


def read_edgeinfo_with_stability_into_db( result = [],
                                          bmark = "",
                                          outdbname = "",
                                          mprflag = False,
                                          stabreader = {},
                                          edgeinfo_config = {},
                                          cycle_cpp_dir = "",
                                          logger = None ):
    assert(logger != None)
    print "A:"
    # print os.listdir( )
    tracefile = os.path.join( cycle_cpp_dir, edgeinfo_config[bmark] )
    # The EdgeInfoFile2DB will create the DB connection. We just
    # need to pass it the DB filename
    edgereader = EdgeInfoFile2DB( edgeinfo_filename = tracefile,
                                  outdbfilename = outdbname,
                                  stabreader = stabreader,
                                  logger = logger )
    print "B:"

def make_adjacency_list( nodes = [],
                         edgelist = [],
                         edgeinfo = None ):
    adjlist = { x : [] for x in nodes }
    nxG = nx.DiGraph()
    for node in nodes:
        nxG.add_node(node)
    for rec in edgelist:
        src = edgeinfo.get_source_id_from_rec(rec)
        tgt = edgeinfo.get_target_id_from_rec(rec)
        # Add to adjacency list
        if src in adjlist:
            adjlist[src].append(tgt)
        else:
            adjlist[src] = [ tgt ]
        if tgt not in adjlist:
            adjlist[tgt] = []
        # Add to networkx graph
        nxG.add_edge(src, tgt)
    return { "adjlist" : adjlist,
             "nxgraph" : nxG }

def get_key_using_last_heap_update( group = [],
                                    graph = {},
                                    objectinfo = {},
                                    logger = None ):
    # Empty?
    lastup_max = max( [ objectinfo.get_last_heap_update(x) for x in group ] )
    candidates = [ x for x in group if objectinfo.get_last_heap_update(x) == lastup_max ]
    if len(candidates) == 1:
        key = candidates[0]
        newgroup = list(group)
        newgroup.remove(key)
    elif len(candidates) > 1:
        logger.warning( "Multiple key objects: %s" % str(candidates) )
        mytypes = [ objectinfo.get_type(x) for x in candidates ]
        logger.warning( " - types: %s\n" % str(mytypes) )
        # Choose one
        lastts_max = max( [ objectinfo.get_last_actual_timestamp(x) for x in candidates ] )
        ts_candidates = [ x for x in candidates if objectinfo.get_last_actual_timestamp(x) == lastts_max ]
        if len(ts_candidates) > 1:
            # sys.stdout.write( " -- Multiple key objects: %s :: " % str(ts_candidates) )
            # sys.stdout.write( " >>> Using last timestamp returned multiple too. Use the oldest one." )
            key = sorted(ts_candidates)[0]
        elif len(ts_candidates) == 1:
            key = ts_candidates[0]
        else:
            # sys.stdout.write( " -- Multiple key objects: %s :: " % str(ts_candidates) )
            # sys.stdout.write( " >>> Using last timestamp didn't return anything. Use the oldest one." )
            key = sorted(candidates)[0]
        newgroup = list(group)
        newgroup.remove(key)
    else:
        raise RuntimeError("No key objects.")
    return (key, newgroup)

def filter_edges( edgelist = [],
                  group = [],
                  edgeinforeader = {} ):
    ei = edgeinforeader
    newedgelist = []
    for edge in edgelist:
            src = ei.get_source_id_from_rec(edge)
            tgt = ei.get_target_id_from_rec(edge)
            if (src in group):
                newedgelist.append(edge)
            else:
                # DEBUG edges
                pass
    return newedgelist

def get_cycle_nodes( edgelist = [] ):
    # Takes an edgelist and returns all the nodes (object IDs)
    # in the edgelist. Uses a set to remove ruplicates.
    nodeset = set()
    for edge in edgelist:
        nodeset.add(edge[0])
        nodeset.add(edge[1])
    return nodeset

def update_cycle_summary( cycle_summary = {},
                          cycledict = {},
                          cyclenode_summary = {},
                          objectinfo = {} ):   
    oi = objectinfo
    for key, nlist in cyclenode_summary.iteritems():
        typelist = list( set( [ oi.get_type(x) for x in nlist ] ) )
        if len(typelist) > 0:
            tup = tuple( sorted( typelist ) )
            cycle_summary[tup] += 1

# This should return a dictionary where:
#     key -> group (list INCLUDES key)
def get_key_objects( group = {},
                     seen_objects = set(),
                     edgeinfo = None,
                     objectinfo = None,
                     cycle_summary = {},
                     # TODO bystack_summary = {},
                     logger = None ):
    latest = 0 # Time of most recent
    tgt = 0
    assert( len(group) > 0 )
    # NOTE:
    # If the group died by stack, then there are no last edges
    #
    # from the heap to speak of. We look for objects without last edges.
    # Get the group death time. This works because by definition, all objects
    # in this group have the same death time.
    dtime = objectinfo.get_death_time( group[0] )
    # Rename:
    ei = edgeinfo
    # Results placed here:
    result = {}
    #     key = Key object ID
    #     value = list of group including key object
    cycledict = {}
    #     key = object ID
    #     value = boolean flag whether the key object is a cycle or not
    #             I think this only keeps track of key object IDS TODO
    cyclenode_summary = {}
    #     key = Key object ID
    #     value = Cycle group list. It SHOULD include the key object ID.
    total_size = 0 # Total size of group
    died_at_end_size = 0
    # Check DIED BY STACK and single
    cause = get_group_died_by_attribute( group = set(group),
                                         objectinfo = objectinfo )
    assert( cause != None and
            ( cause == "HEAP" or
              cause == "STACK" or
              cause == "END" ) )
    if (cause == "STACK") and (len(group) == 1):
        # sys.stdout.write( "DBS 1: %d --" % len(group) )
        # Then this is a key object for a death group by itself
        result = { group[0] : [ group[0] ] }
        edgelist = []
        for obj in group:
            if obj in seen_objects:
                # Dupe. TODO: Log a warning/error
                continue
            # Sum up the size in bytes
            total_size += objectinfo.get_size(obj)
            # Need a get all edges that target 'obj'
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            # TODO: ei.get_source_id_from_rec(x)
            obj_edgelist = [ x for x in srcreclist if
                             (ei.get_death_time_from_rec(x) == dtime) ]
            edgelist.extend( obj_edgelist )
        if len(edgelist) > 0:
            edgelist = filter_edges( edgelist = edgelist,
                                     group = group,
                                     edgeinforeader = ei )
        graph_result = make_adjacency_list( nodes = group,
                                            edgelist = edgelist,
                                            edgeinfo = ei )
        adjlist = graph_result["adjlist"]
        nxgraph = graph_result["nxgraph"]
        for srcnode in result.keys():
            cycledict[srcnode] = False
        for nxnode in nxgraph.nodes():
            # Determine if it has cycles
            if nxnode in result:
                try:
                    cycle_elist = nx.find_cycle(nxgraph, nxnode)
                    cycledict[nxnode] = True
                    cycle_nodes = get_cycle_nodes( cycle_elist )
                except nx.exception.NetworkXNoCycle as e:
                    cycle_elist = []
                    cycledict[nxnode] = False
                    cycle_nodes = set()
                cyclenode_summary[nxnode] = cycle_nodes
                # sys.stdout.write( "   node[%d] -> (%d, %d) cycle %s.\n" %
                #                   ( (nxnode), len(cycle_nodes),
                #                     nxgraph.number_of_edges(),
                #                     ("YES" if cycledict[nxnode] else "NO") ) )
        # TODO: Do we care about cycles of size 1?
    elif (cause == "STACK"):
        assert( len(group) > 1 )
        # sys.stdout.write( "DBS 2: %d" % len(group) )
        # DIED BY STACK
        # We look for all objects that do not have any incoming edge with
        # the same death time as itself. These are the ROOTS.
        none_count = 0
        edgelist = []
        for obj in group:
            # Sum up the size in bytes
            total_size += objectinfo.get_size(obj)
            # TODO: Need a get all edges that target 'obj'
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            # TODO: ei.get_source_id_from_rec(x)
            obj_edgelist = [ x for x in srcreclist if
                             (ei.get_death_time_from_rec(x) == dtime) ]
            if len(obj_edgelist) == 0:
                # Must be a key object
                assert( obj not in result )
                result[obj] = []
            else:
                edgelist.extend( obj_edgelist )
        edgelist = filter_edges( edgelist = edgelist,
                                 group = group,
                                 edgeinforeader = ei )
        # Now use DFS to find subgroups. TODO TODO TODO
        graph_result = make_adjacency_list( nodes = group,
                                            edgelist = edgelist,
                                            edgeinfo = ei )
        adjlist = graph_result["adjlist"]
        nxgraph = graph_result["nxgraph"]
        allobjs = result.keys()
        allobjs.extend( list(chain.from_iterable( adjlist.values() )) )
        allobjs.extend( adjlist.keys() )
        allobjs = remove_dupes(allobjs)
        discovered = { x : False for x in allobjs }
        # Clearly we need to use key objects as the starting points
        if len(result) == 0:
            key, newgroup = get_key_using_last_heap_update( group = group,
                                                            graph = nxgraph,
                                                            objectinfo = objectinfo,
                                                            logger = logger )
            result[key] = newgroup
        for srcnode in result.keys():
            if not discovered[srcnode]:
                mygroup = dfs_iter( G = adjlist,
                                    discovered = discovered,
                                    node = srcnode )
                result[srcnode] = mygroup
        # TODO: At this point all nodes should be discovered. We need to verify. TODO
        for srcnode in result.keys():
            cycledict[srcnode] = False
        for nxnode in nxgraph.nodes():
            # TODO: Determine if it has cycles
            if nxnode in result:
                try:
                    cycle_elist = nx.find_cycle(nxgraph, nxnode)
                    cycledict[nxnode] = True
                    cycle_nodes = get_cycle_nodes( cycle_elist )
                except nx.exception.NetworkXNoCycle as e:
                    cycle_elist = []
                    cycledict[nxnode] = False
                    cycle_nodes = set()
                # sys.stdout.write( "   node[%d] -> (%d, %d) cycle %s.\n" %
                #                   ( (nxnode), len(cycle_nodes),
                #                     len(cycle_elist),
                #                     ("YES" if cycledict[nxnode] else "NO") ) )
                # TODO: Don't need this: sys.stdout.write( "        nodes: %s\n" % str(nxgraph.nodes()) )
    elif cause == "HEAP":
        # DIED BY HEAP and GLOBAL
        # We look for all objects that do not have any incoming edge with
        # the same death time as itself. These are the KEY OBJECTS.
        # In this case there _SHOULD_ only be one KEY OBJECT.
        edgelist = []
        for obj in group:
            # Sum up the size in bytes
            total_size += objectinfo.get_size(obj)
            # TODO: Need a get all edges that target 'obj'
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            # TODO: ei.get_source_id_from_rec(x)
            obj_edgelist = [ x for x in srcreclist if
                             (ei.get_death_time_from_rec(x) == dtime) ]
            if len(obj_edgelist) == 0:
                # Must be a key object
                assert( obj not in result )
                result[obj] = []
            else:
                edgelist.extend( obj_edgelist )
        if len(edgelist) > 0:
            edgelist = filter_edges( edgelist = edgelist,
                                     group = group,
                                     edgeinforeader = ei )
        graph_result = make_adjacency_list( nodes = group,
                                            edgelist = edgelist,
                                            edgeinfo = ei )
        adjlist = graph_result["adjlist"]
        nxgraph = graph_result["nxgraph"]
        if len(result) == 1:
            # The result we expected.
            newgroup = list(group)
            for key in result.keys():
                # TODO newgroup.remove(key)
                result[key] = newgroup
        else:
            # if len(result) > 1:
            #     print "ERROR: multiple key objects."
            #     TODO raise RuntimeError("Multiple key objects.")
            key, newgroup = get_key_using_last_heap_update( group = group,
                                                            graph = nxgraph,
                                                            objectinfo = objectinfo,
                                                            logger = logger )
            result[key] = newgroup
        for srcnode in result.keys():
            cycledict[srcnode] = False
        flag = False
        for nxnode in nxgraph.nodes():
            # TODO: Determine if it has cycles
            if nxnode in result:
                flag = True
                try:
                    cycle_elist = nx.find_cycle(nxgraph, nxnode)
                    cycledict[nxnode] = True
                except nx.exception.NetworkXNoCycle as e:
                    cycle_elist = []
                    cycledict[nxnode] = False
                # sys.stdout.write( "   node[%d] -> (%d, %d) cycle %s.\n" %
                #                   ( (nxnode), nxgraph.number_of_nodes(),
                #                     nxgraph.number_of_edges(),
                #                     ("YES" if cycledict[nxnode] else "NO") ) )
                # sys.stdout.write( "        nodes: %s\n" % str(nxgraph.nodes()) )
        # TODO DEBUG if not flag:
        # TODO DEBUG     print "--------------------------------------------------------------------------------"
        # TODO DEBUG     print "DEBUG: result"
        # TODO DEBUG     pp.pprint( result )
        # TODO DEBUG     print "     : nodes"
        # TODO DEBUG     pp.pprint(nxgraph.nodes())
        # TODO DEBUG     print "--------------------------------------------------------------------------------"
    else:
        if cause == "END":
            # Ignore anything at program end
            for obj in group:
                if obj in seen_objects:
                    # Dupe. TODO: Log a warning/error
                    continue
                # Sum up the size in bytes
                died_at_end_size += objectinfo.get_size(obj)
        else:
            print "***** ERROR: can't classify object - cause[ %s ]*****" % str(cause)
            objectinfo.debug_object(group[0])
        # Either way nothing in the result
    update_cycle_summary( cycle_summary = cycle_summary,
                          cycledict = cycledict,
                          cyclenode_summary = cyclenode_summary,
                          objectinfo = objectinfo )
    return (result, total_size, died_at_end_size, cause)

def main_process( global_config = {},
                  main_config = {},
                  worklist_config = {},
                  host_config = {},
                  dgroups2db_config = {},
                  objectinfo_db_config = {},
                  cachesize_config = {},
                  summary_config = {},
                  # TODO stability_config = {},
                  mprflag = False,
                  debugflag = False,
                  logger = None ):
    global pp, KEY_OBJECT_SUMMARY
    # This is where the summary CSV files are. We get the
    # bmark-CYCLES.csv files here.
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Get the date and time to label the work directory.
    today = date.today()
    today = today.strftime("%Y-%m%d")
    timenow = datetime.now().time().strftime("%H-%M-%S")
    olddir = os.getcwd()
    os.chdir( main_config["output"] )
    workdir = create_work_directory( work_dir = main_config["output"],
                                     today = today,
                                     timenow = timenow,
                                     logger = logger,
                                     interactive = False )
    os.chdir( workdir )
    # Timestamped work directories are not deleted unless low
    # in space. This is to be able to go back to a known good dataset.
    # The current run is then copied to a non-timestamped directory
    # where the rest of the workflow expects it as detailed in the config file.
    # Directory is in "global_config"
    assert( "cycle_cpp_dir" in global_config )
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    manager = Manager()
    results = {}
    cycle_summary_all = {}
    procs_dgroup = {}
    dblist = []
    # Print out
    with open(KEY_OBJECT_SUMMARY, mode = "wb") as key_summary_fp:
        # Key object general statistics
        key_summary_writer = csv.writer(key_summary_fp)
        header = [ "benchmark", "alloc-total", "actual-alloc-total",
                   "dsite_1", "dsite_1-total", "dsite_1-%", "dsite_1-by-stack-%",  "dsite_1-by-heap-%", "number-groups",
                              "dsite_1-min-alloc-age", "dsite_1-max-alloc-age", "dsite_1-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_2", "dsite_2-total", "dsite_2-%", "dsite_2-by-stack-%",  "dsite_2-by-heap-%", "number-groups",
                              "dsite_2-min-alloc-age", "dsite_2-max-alloc-age", "dsite_2-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_3", "dsite_3-total", "dsite_3-%", "dsite_3-by-stack-%",  "dsite_3-by-heap-%", "number-groups",
                              "dsite_3-min-alloc-age", "dsite_3-max-alloc-age", "dsite_3-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_4", "dsite_4-total", "dsite_4-%", "dsite_4-by-stack-%",  "dsite_4-by-heap-%", "number-groups",
                              "dsite_4-min-alloc-age", "dsite_4-max-alloc-age", "dsite_4-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_5", "dsite_5-total", "dsite_5-%", "dsite_5-by-stack-%",  "dsite_5-by-heap-%", "number-groups",
                              "dsite_5-min-alloc-age", "dsite_5-max-alloc-age", "dsite_5-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                              ]
        key_summary_writer.writerow(header)
        for bmark in worklist_config.keys():
            hostlist = worklist_config[bmark]
            if not check_host( benchmark = bmark,
                               hostlist = hostlist,
                               host_config = host_config ):
                continue
            # Else we can run for 'bmark'
            cachesize = int(cachesize_config[bmark])
            if mprflag:
                print "=======[ Spawning %s ]================================================" \
                    % bmark
                results[bmark] = Queue()
                cycle_summary_all[bmark] = Queue()
                # NOTE: The order of the args tuple is important!
                # ======================================================================
                # Read in the death groups from dgroups2db.py
                p = Process( target = read_dgroups_from_pickle,
                             args = ( results[bmark],
                                      bmark,
                                      workdir,
                                      True, # mprflag
                                      dgroups2db_config,
                                      objectinfo_db_config,
                                      summary_config,
                                      cycle_cpp_dir,
                                      cachesize,
                                      debugflag,
                                      cycle_summary_all[bmark],
                                      logger ) )
                procs_dgroup[bmark] = p
                p.start()
            else:
                print "=======[ Running %s ]=================================================" \
                    % bmark
                print "     Reading in death groups..."
                results[bmark] = []
                cycle_summary_all[bmark] = []
                read_dgroups_from_pickle( result = results[bmark],
                                          bmark = bmark,
                                          workdir = workdir,
                                          mprflag = False,
                                          dgroups2db_config = dgroups2db_config,
                                          objectinfo_db_config = objectinfo_db_config,
                                          summary_config = summary_config,
                                          cycle_cpp_dir = cycle_cpp_dir,
                                          obj_cachesize = cachesize,
                                          debugflag = debugflag,
                                          cycle_result = cycle_summary_all[bmark],
                                          logger = logger )
                for row in results[bmark]:
                    key_summary_writer.writerow( row )
                key_summary_fp.flush()
                os.fsync(key_summary_fp.fileno())
            # Copy file from workdir
            srcpath = os.path.join( workdir, KEY_OBJECT_SUMMARY )
            # Check to see if filename exists in workdir
            tgtpath = os.path.join( main_config["output"], KEY_OBJECT_SUMMARY )
            if os.path.isfile(tgtpath):
                # Moving the older into BAK directory
                bakfilename = "%s-%s-%s" % (today, timenow, KEY_OBJECT_SUMMARY)
                TEMPpath = os.path.join( main_config["output"], "BAK" )
                bakpath = os.path.join( TEMPpath, bakfilename )
                # And check the bakpath
                if os.path.isfile( bakpath ):
                    # Remove if it's there
                    os.remove( bakpath )
                move( tgtpath, bakpath )
            copy( srcpath, tgtpath )
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
                        while not results[bmark].empty():
                            row = results[bmark].get()
                            key_summary_writer.writerow( row )
                        key_summary_fp.flush()
                        os.fsync(key_summary_fp.fileno())
                        timenow = time.asctime()
                        logger.debug( "[%s] - done at %s" % (bmark, timenow) )
            print "======[ Processes DONE ]========================================================"
            sys.stdout.flush()
            # TODO: Need to output cycle information
        else:
            # TODO TEMPORARY OUTPUT
            print "=====[ Cycle Summary ]=========================================================="
            pp.pprint( cycle_summary_all )
            print "=====[ END Cycle Summary ]======================================================"
    print "================================================================================"
    print "DONE."
    exit(0)
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
    print "analyze_dgroup.py - DONE."
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
    main_config = config_section_map( "analyze-dgroup", config_parser )
    host_config = config_section_map( "hosts", config_parser )
    # Reuse the dgroups2db-worklist
    worklist_config = config_section_map( "dgroups2db-worklist", config_parser )
    # We take the file output of dgroups2db as input
    dgroups2db_config = config_section_map( "dgroups2db", config_parser )
    objectinfo_db_config = config_section_map( "objectinfo-db", config_parser )
    # Reuse the cachesize
    cachesize_config = config_section_map( "create-supergraph-obj-cachesize", config_parser )
    # Summary files
    summary_config = config_section_map( "summary-cpp", config_parser )
    # MAYBE: edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "worklist" : worklist_config,
             "hosts" : host_config,
             "dgroups2db" : dgroups2db_config,
             "objectinfo_db" : objectinfo_db_config,
             "cachesize" : cachesize_config,
             "summary" : summary_config,
             }

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
    parser.add_argument( "--mpr",
                         dest = "mprflag",
                         help = "Enable multiprocessing.",
                         action = "store_true" )
    parser.add_argument( "--single",
                         dest = "mprflag",
                         help = "Single threaded operation.",
                         action = "store_false" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "analyze_dgroup.log",
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
    worklist_config = process_worklist_config( configdict["worklist"] )
    host_config = process_host_config( configdict["hosts"] )
    dgroups2db_config = configdict["dgroups2db"]
    objectinfo_db_config = configdict["objectinfo_db"]
    cachesize_config = configdict["cachesize"]
    summary_config = configdict["summary"]
    # TODO objectinfo_config = configdict["objectinfo"]
    # TODO stability_config = configdict["stability"]
    # TODO DEBUG TODO
    print "================================================================================"
    pp.pprint( global_config )
    print "================================================================================"
    pp.pprint( main_config )
    print "================================================================================"
    pp.pprint( host_config )
    print "================================================================================"
    pp.pprint( dgroups2db_config )
    # TODO END DEBUG TODO
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         global_config = global_config,
                         main_config = main_config,
                         host_config = host_config,
                         worklist_config = worklist_config,
                         dgroups2db_config = dgroups2db_config,
                         objectinfo_db_config = objectinfo_db_config,
                         cachesize_config = cachesize_config,
                         summary_config = summary_config,
                         # MAYBE stability_config = stability_config,
                         mprflag = args.mprflag,
                         logger = logger )

if __name__ == "__main__":
    main()
