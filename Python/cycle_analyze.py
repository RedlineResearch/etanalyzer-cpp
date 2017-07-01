from __future__ import division
# cycle_analyze.py
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
RAW_KEY_OBJECT_FILENAME = "raw-key-object-summary.csv"
CYCLE_FILENAME = "cycle-summary.csv"
RAW_CYCLE_FILENAME = "raw-cycle-summary.csv"

def setup_logger( targetdir = ".",
                  filename = "cycle_analyze.py.log",
                  logger_name = 'cycle_analyze.py',
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
    assert( dsite != None and nonjdsite != None )
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
        # Update counter
        agedict[mydsite]["counter"].update( age_list )

def update_group_summaries( glist_len = None,
                            dsite_dict = {},
                            nonjlib_dict = {} ):
    assert( glist_len != None )
    dsite_dict["min"] = \
        min( glist_len, dsite_dict["min"] ) if (dsite_dict["min"] != 0) \
        else glist_len
    nonjlib_dict["min"] = \
        min( glist_len, nonjlib_dict["min"] ) if (nonjlib_dict["min"] != 0) \
        else glist_len
    dsite_dict["max"] = max( glist_len, dsite_dict["max"] )
    nonjlib_dict["max"] = max( glist_len, nonjlib_dict["max"] )
    # Get running total/sum
    #   DSite
    old_count, old_total = dsite_dict["gensum"]
    dsite_dict["gensum"] = (old_count + 1, old_total + glist_len)
    #   Nonjlib
    old_count, old_total = nonjlib_dict["gensum"]
    nonjlib_dict["gensum"] = (old_count + 1, old_total + glist_len)

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
    dsites_gstats = summary["DSITES_GROUP_STATS"]
    dsites_distr = summary["DSITES_DISTRIBUTION"]
    dsites_age = summary["DSITES_AGE"]
    nonjlib_dsitesdict = summary["NONJLIB_DSITES"]
    nonjlib_dsites_size = summary["NONJLIB_SIZE"]
    nonjlib_gcount = summary["NONJLIB_GROUP_COUNT"]
    nonjlib_gstats = summary["NONJLIB_GROUP_STATS"]
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
        update_group_summaries( glist_len = len(glist),
                                dsite_dict = dsites_gstats[dsite],
                                nonjlib_dict = nonjlib_gstats[nonjdsite] )

def encode_row( row = [] ):
    newrow = []
    for item in row:
        if type(item) == int:
            newrow.append(item)
        elif type(item) == str:
            newrow.append( item.encode('utf-8') )
        else:
            newrow.append(str(item))
    return newrow

def raw_output_to_csv( key = None,
                       subgroup = [],
                       raw_writer = None,
                       objectinfo = {},
                       total_size = 0,
                       logger = None ):
    assert( key != None )
    assert( key in subgroup ) # TODO DEBUG TEMPORARY ONLY
    assert( len(set(subgroup)) == len(subgroup) )  # TODO DEBUG TEMPORARY ONLY
    assert( raw_writer != None )
    # subgroup.insert( 0, key )
    # Rename objectinfo
    oi = objectinfo
    # Get object record
    keyrec = oi.get_record( key )
    # Type
    keytype = oi.get_type_using_record( keyrec )
    # Key alloc age
    keyage = oi.get_age_using_record_ALLOC( keyrec )
    # Oldest object age in the group
    oldest_age = max( [ oi.get_age_ALLOC(x) for x in subgroup ] )
    # Key object allocation method
    key_alloc_site = oi.get_allocsite_using_record( keyrec )
    #  - and the non Java library portion of the allocation context
    key_nonjlib_alloc_site = oi.get_non_javalib_alloc_sitename_using_record( keyrec )
    # Death cause of key object
    cause = oi.get_death_cause_using_record( keyrec )
    if cause == "S":
        # By STACK:
        cause = "SHEAP" if oi.died_by_stack_after_heap_using_record( keyrec ) \
            else "STACK"
    elif ( cause == "H" or cause == "G" ):
        # By HEAP. TODO
        cause = "HEAP"
    elif cause == "E":
        # By program end, which should have been ignored.
        # Log a WARNING. TODO
        # We don't care about things that are immortal.
        logger.warning( "PROG END type objects shouldn't make it into this function." )
        logger.warning( "ojbId[ %d ] type[ %s ] allocsite[ %s ]" %
                        (key, keytype, key_alloc_site) )
        return
    else:
        raise RuntimeError("Unexpected death cause: %s" % cause)
    # Death contexts
    dcont1 = oi.get_death_context_using_record( keyrec )
    dcont2 = oi.get_death_context_L2_using_record( keyrec )
    # TODO: The function get_non_javalib_context_using_record() refers
    #       to part of the death context. The name should make this clear.
    #   TODO: Change the name in garbology.py and then change it here.
    nonjavalib_death_cont = oi.get_non_javalib_context_using_record( keyrec )
    # Pointed at by heap flag
    # TODO: This is a correct hack. If the object was ever pointed at by the heap,
    #       then it can only be either HEAP or SHEAP. Since we ignored immortal
    #       objects that died at program's end, we don't need to account for those.
    pointed_at_by_heap = (cause == "HEAP" or cause == "SHEAP")
    row = [ key, len(subgroup), total_size,
            keytype, keyage, key_alloc_site, key_nonjlib_alloc_site, oldest_age,
            cause, dcont1, dcont2, nonjavalib_death_cont,
            str(pointed_at_by_heap) ]
    # TODO raw_writer.writerow(row)
    newrow = encode_row(row)
    try:
        raw_writer.writerow(newrow)
    except:
        print "%d : %s = %s" % (key, keytype, type(key_nonjlib_alloc_site))
        print "Encoded row:"
        pp.pprint(newrow)
        exit(100)

def output_cycle_summary_to_csv( typetup = {},
                                 age_rec = {},
                                 cpair_rec = {},
                                 cycle_writer = None,
                                 bmark = None,
                                 logger = None ):
    assert( len(typetup) > 0 )
    assert( type(typetup) == tuple )
    assert( len(age_rec) > 0 )
    assert( cycle_writer != None )
    assert( bmark != None )
    print "[%s TUP] - %s " % (bmark, typetup)
    print "   - age : %s" % str(age_rec)
    # TODO: row = encode_row([ typetup,
    # TODO:                    groupsize,
    # TODO:                    age_rec["mean"],
    # TODO:                    age_rec["range"], ]) )
    newrow = encode_row(row)
    try:
        cycle_writer.writerow(newrow)
    except:
        print "%d : %s = %s" % (str(typetup), type(key_nonjlib_alloc_site))
        print "Encoded row:"
        pp.pprint(newrow)
        exit(100)

def new_cycle_age_record( new_min = None,
                          new_max = None,
                          age_range = None,
                          age_mean = None,
                          count = None ):
    assert( age_mean != None ) # TODO DEBUG ONLY
    return { "min" : new_min,
             "max" : new_max,
             "range" : age_range,
             "mean" : age_mean,
             "count" : count, }

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
    print "Reading in the DGROUPS file[ %s ] for %s:" % (picklefile, bmark)
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
    cycle_age_summary = defaultdict( lambda: [] )
    cycle_cpair_summary = defaultdict( lambda: [] )
    count = 0
    key_objects = { "GROUPLIST" : {},
                    "CAUSE" : Counter(),
                    "DSITES" : Counter(),
                    "DSITES_SIZE" : defaultdict(int),
                    "DSITES_GROUP_COUNT" : Counter(),
                    "DSITES_GROUP_STATS" :
                        defaultdict( lambda: { "max" : 0, "min" : 0, "ave" : 0, "gensum" : (0, 0) } ),
                    "DSITES_DISTRIBUTION" :
                        defaultdict( lambda: defaultdict(int) ),
                    "DSITES_AGE" :
                        defaultdict( lambda: { "max" : 0, "min" : 0, "ave" : 0, "gensum" : (0, 0),
                                               "counter" : Counter () } ),
                    "NONJLIB_DSITES" : Counter(),
                    "NONJLIB_SIZE" : defaultdict(int),
                    "NONJLIB_GROUP_COUNT" : Counter(),
                    "NONJLIB_GROUP_STATS" :
                        defaultdict( lambda: { "max" : 0, "min" : 0, "ave" : 0, "gensum" : (0, 0) } ),
                    "NONJLIB_DISTRIBUTION" :
                        defaultdict( lambda: defaultdict(int) ),
                    "NONJLIB_AGE" :
                        defaultdict( lambda: { "max" : 0, "min" : 0, "ave" : 0, "gensum" : (0, 0),
                                               "counter" : Counter (), } ),
                    "TYPES" : Counter(),
                    "TYPE_DSITES" : defaultdict(Counter),
                    "TOTAL_SIZE" : {}, }
    seen_objects = set()
    total_alloc = 0
    total_died_at_end_size = 0
    rawpath = os.path.join( workdir, bmark + "-" + RAW_KEY_OBJECT_FILENAME )
    cyclepath = os.path.join( workdir, bmark + "-" + CYCLE_FILENAME )
    raw_cyclepath = os.path.join( workdir, bmark + "-" + RAW_CYCLE_FILENAME )
    # TODO: Raw death groups: keep in original cycle_analyze.py? or also do here?
    #       Leaning towards keeping it separate in cycle_analyze.py.
    # with open( rawpath, "wb" ) as fpraw, \
    with open( cyclepath, "wb" ) as cycfp, \
        open( raw_cyclepath, "wb" ) as raw_cycfp:
        #----------------------------------------------------------------------
        # TODO: Not needed in this script. This is in analyze_dgroup.py
        # TODO # Raw groups
        # TODO raw_writer = csv.writer( fpraw,
        # TODO                          quoting = csv.QUOTE_NONNUMERIC )
        # TODO key_header = [ "objectId", "number-objects", "size-group",
        # TODO                "key-type", "key-alloc-age", "key-alloc-site", "alloc-non-Java-lib-context", "oldest-member-age",
        # TODO                "death-cause", "death-context-1", "death-context-2", "non-Java-lib-context",
        # TODO                "pointed-at-by-heap", ]
        # TODO raw_writer.writerow( key_header )
        #----------------------------------------------------------------------

        # RAW cycle file
        raw_cycle_header = [ "type-tuple", ] # TODO TODO TODO TODO
        raw_cycle_writer = csv.writer( raw_cycfp,
                                       quoting = csv.QUOTE_NONNUMERIC )
        raw_cycle_writer.writerow( raw_cycle_header )
        # Cycle summary file
        cycle_header = [ "type-tuple", "cycles-size",
                         "mean-age", "range-age", ] # TODO TODO TODO TODO
        cycle_writer = csv.writer( cycfp,
                                   quoting = csv.QUOTE_NONNUMERIC )
        cycle_writer.writerow( cycle_header )
        for gnum, glist in dgroups_data["group2list"].iteritems():
            # - for every death group dg:
            #       get the last edge for every object
            cyclelist, total_size, died_at_end_size, cause = get_cycles( group = glist,
                                                                         seen_objects = seen_objects,
                                                                         edgeinfo = edgeinfo,
                                                                         objectinfo = objectinfo,
                                                                         cycle_summary = cycle_summary,
                                                                         cycle_age_summary = cycle_age_summary,
                                                                         cycle_cpair_summary = cycle_cpair_summary,
                                                                         raw_cycle_writer = raw_cycle_writer,
                                                                         logger = logger )
            if cause == "END":
                assert( died_at_end_size > 0 )
                total_died_at_end_size += died_at_end_size
            elif len(cyclelist) > 0:
                assert( (cause == "HEAP") or (cause == "STACK") )
                total_alloc += total_size
                # TODO: HERE 30 June 2017
        print "Cycles total = ", len(cycle_age_summary)
        for typetup, age_rec in cycle_age_summary.iteritems():
            # Summary of key types
            output_cycle_summary_to_csv( typetup = typetup,
                                         age_rec = age_rec,
                                         cpair_rec = {}, # TODO TODO TODO HERE
                                         cycle_writer = cycle_writer,
                                         bmark = bmark,
                                         logger = logger )
                # END TODO: HERE 30 June 2017
                # TODO update_key_object_summary( newgroup = key_result,
                # TODO                            summary = key_objects,
                # TODO                            objectinfo = objectinfo,
                # TODO                            total_size = total_size,
                # TODO                            logger = logger )
                # TODO # TODO DEBUG print "%d: %s" % (count, key_result)
                # TODO ktc = keytype_counter # Short alias
                # TODO for key, subgroup in key_result.iteritems():
                # TODO     count += 1
                # TODO     # Summary of key types
                # TODO     keytype = objectinfo.get_type(key)
                # TODO     ktc[keytype] += 1
                # TODO     groupsize = len(subgroup)
                # TODO     # Summary of death locations
                # TODO     deathsite = objectinfo.get_death_context(key)
                # TODO     dss[keytype][deathsite] += 1
                # TODO     raw_output_to_csv( key = key,
                # TODO                        subgroup = subgroup,
                # TODO                        raw_writer = raw_writer,
                # TODO                        objectinfo = objectinfo,
                # TODO                        total_size = total_size,
                # TODO                        logger = logger )
    # TODO HERE exit(1000)
    # TODO # Save the CSV file the key object summary
    # TODO total_objects = summary_reader.get_number_of_objects()
    # TODO # TODO num_key_objects = len(key_objects["GROUPLIST"])
    # TODO size_died_at_end = summary_reader.get_size_died_at_end()
    # TODO size_total_allocation = summary_reader.get_final_garbology_alloc_time()
    # TODO # TODO dsites_gcount = key_objects["DSITES_GROUP_COUNT"]
    # TODO #-------------------------------------------------------------------------------
    # TODO # Alloc age computation TODO TODO TODO
    # TODO # Max and min have been maintained for each death site so nothing needed here.
    # TODO # Update the averages/mean:
    # TODO dsites_age = key_objects["DSITES_AGE"]
    # TODO dsites_gstats = key_objects["DSITES_GROUP_STATS"]
    # TODO for mydsite in dsites_age.keys():
    # TODO     count, total = dsites_age[mydsite]["gensum"]
    # TODO     dsites_age[mydsite]["ave"] = (total / count) if count > 0 \
    # TODO         else 0
    # TODO     gcount, gtotal = dsites_gstats[mydsite]["gensum"]
    # TODO     dsites_gstats[mydsite]["ave"] = (gtotal / gcount) if gcount > 0 \
    # TODO         else 0
    # TODO nonjlib_age = key_objects["NONJLIB_AGE"]
    # TODO nonjlib_gstats = key_objects["NONJLIB_GROUP_STATS"]
    # TODO for mydsite in nonjlib_age.keys():
    # TODO     count, total = nonjlib_age[mydsite]["gensum"]
    # TODO     assert( count > 0 )
    # TODO     nonjlib_age[mydsite]["ave"] = total / count
    # TODO     gcount, gtotal = nonjlib_gstats[mydsite]["gensum"]
    # TODO     nonjlib_gstats[mydsite]["ave"] = (gtotal / gcount) if gcount > 0 \
    # TODO         else 0
    # TODO #-------------------------------------------------------------------------------
    # TODO # First level death sites
    # TODO total_alloc_MB = bytes_to_MB(total_alloc)
    # TODO actual_alloc = total_alloc - total_died_at_end_size
    # TODO actual_alloc_MB = bytes_to_MB(actual_alloc)
    # TODO newrow = [ (bmark, total_alloc_MB, actual_alloc_MB) ]
    # TODO # Get the top 5 death sites
    # TODO #     * Use "DSITES_SIZE" and sort. Get top 5.
    # TODO dsites_distr = key_objects["DSITES_DISTRIBUTION"]
    # TODO dsites_size = sorted( key_objects["DSITES_SIZE"].items(),
    # TODO                       key = itemgetter(1),
    # TODO                       reverse = True )
    # TODO newrow = newrow + [ ( x[0], bytes_to_MB(x[1]), ((x[1]/actual_alloc) * 100.0),
    # TODO                       ((dsites_distr[x[0]]["STACK"]/x[1]) * 100.0),
    # TODO                       ((dsites_distr[x[0]]["HEAP"]/x[1]) * 100.0),
    # TODO                       dsites_gcount[x[0]],
    # TODO                       dsites_gstats[x[0]]["min"], dsites_gstats[x[0]]["max"], dsites_gstats[x[0]]["ave"],
    # TODO                       dsites_age[x[0]]["min"], dsites_age[x[0]]["max"],  dsites_age[x[0]]["ave"], )
    # TODO                     for x in dsites_size[0:5] ]
    # TODO newrow = [ x for tup in newrow for x in tup ]
    # TODO #-------------------------------------------------------------------------------
    # TODO # First non Java library function
    # TODO # TODO TODO: Add allocation total
    # TODO newrow_nonjlib = [ ((bmark + " NONJ"), total_alloc_MB, actual_alloc_MB), ]
    # TODO # Get the top 5 death sites
    # TODO #     * Use "NONJLIB_SIZE" and sort. Get top 5.
    # TODO nonjlib_distr = key_objects["NONJLIB_DISTRIBUTION"]
    # TODO nonjlib_gcount = key_objects["NONJLIB_GROUP_COUNT"]
    # TODO nonjlib_dsites_size = sorted( key_objects["NONJLIB_SIZE"].items(),
    # TODO                               key = itemgetter(1),
    # TODO                               reverse = True )
    # TODO # TODO TODO Add dsite-total, dsite percentage using MB
    # TODO dsites_age = key_objects["DSITES_AGE"]
    # TODO nonjlib_age = key_objects["NONJLIB_AGE"]
    # TODO newrow_nonjlib = newrow_nonjlib + [ ( x[0], bytes_to_MB(x[1]), ((x[1]/actual_alloc) * 100.0),
    # TODO                                       ((nonjlib_distr[x[0]]["STACK"]/x[1]) * 100.0),
    # TODO                                       ((nonjlib_distr[x[0]]["HEAP"]/x[1]) * 100.0),
    # TODO                                       nonjlib_gcount[x[0]],
    # TODO                                       nonjlib_gstats[x[0]]["min"], nonjlib_gstats[x[0]]["max"], nonjlib_gstats[x[0]]["ave"],
    # TODO                                       nonjlib_age[x[0]]["min"], nonjlib_age[x[0]]["max"],  nonjlib_age[x[0]]["ave"], )
    # TODO                                     for x in nonjlib_dsites_size[0:5] ]
    # TODO newrow_nonjlib = [ x for tup in newrow_nonjlib for x in tup ]
    #-------------------------------------------------------------------------------
    # TODO DELETE DEBUG print "X:", newrow
    # Write out the row
    # TODO: REMOVE: key_summary_writer.writerow( newrow )
    # TODO: REMOVE: key_summary_writer.writerow( newrow_nonjlib )
    # TODO: if not mprflag:
    # TODO:     result.append( newrow )
    # TODO:     result.append( newrow_nonjlib )
    # TODO:     cycle_result.append( cycle_summary )
    # TODO: else:
    # TODO:     result.put( newrow )
    # TODO:     result.put( newrow_nonjlib )
    # TODO:     cycle_result.put( cycle_summary )
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
        # NOTE: If the node were not in nodes for some reason, this will add it anyway.
    return { "adjlist" : adjlist,
             "nxgraph" : nxG }

def get_key_using_last_heap_update( group = [],
                                    graph = {},
                                    objectinfo = {},
                                    result = {},
                                    logger = None ):
    # Empty?
    lastup_max = max( [ objectinfo.get_last_heap_update(x) for x in group ] )
    candidates = [ x for x in group if objectinfo.get_last_heap_update(x) == lastup_max ]
    if len(candidates) == 1:
        result[candidates[0]] = []
    elif len(candidates) > 1:
        # Choose one
        lastts_max = max( [ objectinfo.get_last_actual_timestamp(x) for x in candidates ] )
        ts_candidates = [ x for x in candidates if objectinfo.get_last_actual_timestamp(x) == lastts_max ]
        # Note that if the group died by STACK
        assert( len(ts_candidates) > 0 )
        for cand in ts_candidates:
            result[cand] = []
    else:
        raise RuntimeError("No key objects.")

def filter_edges( edgelist = [],
                  group = [],
                  edgeinforeader = {} ):
    ei = edgeinforeader
    newedgelist = []
    for edge in edgelist:
        src = ei.get_source_id_from_rec(edge)
        # TODO: tgt = ei.get_target_id_from_rec(edge)
        if (src in group):
            newedgelist.append(edge)
        # else:
        #     # DEBUG edges
        #     pass
    return newedgelist

def get_cycle_deathsite( cycle = [],
                         objectinfo = {} ):
    dsites = Counter()
    oi = objectinfo
    for node in cycle:
        ds = oi.get_death_context(node)
        dsites[ds] += 1
    mylist = dsites.most_common(1)
    if len(mylist) == 1:
        return mylist[0][0]
    else:
        assert(False)

def get_cycle_nodes( edgelist = [] ):
    # Takes an edgelist and returns all the nodes (object IDs)
    # in the edgelist. Uses a set to remove ruplicates.
    nodeset = set()
    for edge in edgelist:
        nodeset.add(edge[0])
        nodeset.add(edge[1])
    return nodeset

def get_cycle_age_stats( cycle = [],
                         objectinfo = {} ):
    oi = objectinfo
    age_list = [ oi.get_age_ALLOC(objId) for objId in cycle ]
    age_list = filter( lambda x: x != 0,
                       age_list )
    if len(age_list) == 0:
        return None # TODO TODO TODO
    new_min = min(age_list)
    new_max = max(age_list)
    age_range = new_max - new_min
    age_mean = mean(age_list)
    return new_cycle_age_record( new_min = new_min,
                                 new_max = new_max,
                                 age_range = age_range,
                                 age_mean = age_mean,
                                 count = len(cycle) )

def update_cycle_summary( cycle_summary = {},
                          cycledict = {},
                          cyclelist = [],
                          objectinfo = {},
                          cycle_age_summary = {},
                          cycle_cpair_summary = {},
                          raw_cycle_writer = None,
                          groupsize = 0,
                          logger = None ):
    oi = objectinfo # Just a rename
    for nlist in cyclelist:
        typelist = []
        for node in nlist:
            cycledict[node] = True
            mytype = oi.get_type(node)
            typelist.append( mytype )
        typelist = list( set(typelist) )
        assert( len(typelist) > 0 ) # TODO. Only a debugging assert
        # Use a default order to prevent duplicates in tuples
        tup = tuple( sorted( typelist ) )
        # Save count
        cycle_summary[tup] += 1
        # Get the death site
        dsite = get_cycle_deathsite( nlist,
                                     objectinfo = oi )
        # cycle_age_summary:
        #    key: type tuple
        #    value: list of cycle age summary records
        age_rec = get_cycle_age_stats( cycle = nlist,
                                       objectinfo = oi )
        if age_rec != None:
            cycle_age_summary[tup].append( age_rec  )
            # TODO Death site, allocation site TODO
            raw_cycle_writer.writerow( encode_row([ tup,
                                                    groupsize,
                                                    age_rec["mean"],
                                                    age_rec["range"], ]) )
        else:
            logger.error( "TODO: Unable to save cycle %s" % str(cyclelist) )

# This should return a dictionary where:
#     key -> group (list INCLUDES key)
def get_cycles( group = {},
                seen_objects = set(),
                edgeinfo = None,
                objectinfo = None,
                cycle_summary = {},
                cycle_age_summary = {},
                cycle_cpair_summary = {},
                raw_cycle_writer = None,
                logger = None ):
    latest = 0 # Time of most recent
    tgt = 0
    assert( len(group) > 0 )
    dtime = objectinfo.get_death_time( group[0] )
    # Rename:
    ei = edgeinfo
    # Results placed here:
    cyclelist = [] # List of cycles
    cycledict = {} # node -> True/False if cycle
    total_size = 0 # Total size of group
    died_at_end_size = 0
    # Get the death cause
    cause = get_group_died_by_attribute( group = set(group),
                                         objectinfo = objectinfo )
    assert( cause != None and
            ( cause == "HEAP" or
              cause == "STACK" or
              cause == "END" ) ) # These are the only valid final states.
    if ( (len(group) == 1) and
         (cause != "END") ):
        # sys.stdout.write( "SIZE 1: %d --" % len(group) )
        # Group of 1 and it didn't die at the end.
        obj = group[0]
        if obj not in seen_objects:
            # Sum up the size in bytes
            objsize = objectinfo.get_size(obj)
            total_size += objsize
            # Get all edges that target 'obj':
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            # TODO what is this for? TODO: ei.get_source_id_from_rec(x)
            edgelist = [ x for x in srcreclist if
                         (ei.get_death_time_from_rec(x) == dtime) ]
            # if len(edgelist) > 0:
            #     edgelist = filter_edges( edgelist = edgelist,
            #                              group = group,
            #                              edgeinforeader = ei )
            if len(edgelist) > 0:
                for edge in edgelist:
                    src = ei.get_source_id_from_rec(edge)
                    if src == obj:
                        # This means there's a self-cycle of 1 node:
                        cyclelist = [ [ obj, ] ]
                        update_cycle_summary( cycle_summary = cycle_summary,
                                              cycledict = cycledict,
                                              cyclelist = cyclelist,
                                              objectinfo = objectinfo,
                                              cycle_age_summary = cycle_age_summary,
                                              raw_cycle_writer = raw_cycle_writer,
                                              cycle_cpair_summary = cycle_cpair_summary,
                                              groupsize = objsize,
                                              logger = logger )
                        break
        return ( cyclelist,
                 total_size, # size of group in bytes
                 0, # died at end size (known to be 0)
                 cause ) # death cause
    elif cause != "END":
        assert( len(group) > 1 )
        edgelist = []
        groupsize = 0
        for obj in group:
            # Sum up the size in bytes
            objsize = objectinfo.get_size(obj)
            total_size += objsize
            groupsize += objsize
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            # TODO what is this for? TODO: ei.get_source_id_from_rec(x)
            obj_edgelist = [ x for x in srcreclist if
                             (ei.get_death_time_from_rec(x) == dtime) ]
            if len(obj_edgelist) > 0:
                edgelist.extend( obj_edgelist )
        # edgelist = filter_edges( edgelist = edgelist,
        #                          group = group,
        #                          edgeinforeader = ei )
        graph_result = make_adjacency_list( nodes = group,
                                            edgelist = edgelist,
                                            edgeinfo = ei )
        adjlist = graph_result["adjlist"]
        nxgraph = graph_result["nxgraph"]
        cycle_nodes = set()
        cycledict = { n : False for n in nxgraph.nodes() }
        cycles_gen = nx.simple_cycles(nxgraph)
        for elem in cycles_gen:
            new_cycle = []
            for nxnode in elem:
                cycle_nodes.add(nxnode)
                assert(nxnode in cycledict)
                cycledict[nxnode] = True
                new_cycle.append( nxnode )
            if len(new_cycle) > 0:
                cyclelist.append( new_cycle )
            # else do a DEBUG? Shouldn't have an empty cycle here. TODO
        if len(cyclelist) > 0:
            update_cycle_summary( cycle_summary = cycle_summary,
                                  cycledict = cycledict,
                                  cyclelist = cyclelist,
                                  objectinfo = objectinfo,
                                  cycle_age_summary = cycle_age_summary,
                                  raw_cycle_writer = raw_cycle_writer,
                                  cycle_cpair_summary = cycle_cpair_summary,
                                  groupsize = groupsize,
                                  logger = logger )
        return ( cyclelist,
                 total_size,
                 died_at_end_size,
                 cause )
    else:
        assert( cause == "END" )
        # Ignore anything at program end
        for obj in group:
            if obj in seen_objects:
                # Dupe. TODO: Log a warning/error
                continue
            # Sum up the size in bytes
            died_at_end_size += objectinfo.get_size(obj)
        return (cyclelist, total_size, died_at_end_size, cause)

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
                              "dsite_1-group-min", "dsite_1-group-max", "dsite_1-group-ave",
                              "dsite_1-min-alloc-age", "dsite_1-max-alloc-age", "dsite_1-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_2", "dsite_2-total", "dsite_2-%", "dsite_2-by-stack-%",  "dsite_2-by-heap-%", "number-groups",
                              "dsite_2-group-min", "dsite_2-group-max", "dsite_2-group-ave",
                              "dsite_2-min-alloc-age", "dsite_2-max-alloc-age", "dsite_2-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_3", "dsite_3-total", "dsite_3-%", "dsite_3-by-stack-%",  "dsite_3-by-heap-%", "number-groups",
                              "dsite_3-group-min", "dsite_3-group-max", "dsite_3-group-ave",
                              "dsite_3-min-alloc-age", "dsite_3-max-alloc-age", "dsite_3-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_4", "dsite_4-total", "dsite_4-%", "dsite_4-by-stack-%",  "dsite_4-by-heap-%", "number-groups",
                              "dsite_4-group-min", "dsite_4-group-max", "dsite_4-group-ave",
                              "dsite_4-min-alloc-age", "dsite_4-max-alloc-age", "dsite_4-ave-alloc-age",
                              # TODO young vs old (total count or percentage?)
                   "dsite_5", "dsite_5-total", "dsite_5-%", "dsite_5-by-stack-%",  "dsite_5-by-heap-%", "number-groups",
                              "dsite_5-group-min", "dsite_5-group-max", "dsite_5-group-ave",
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
                        print "DONE: %s" % bmark
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
    # TODO # Copy all the databases into MAIN directory.
    # TODO dest = main_config["output"]
    # TODO for filename in os.listdir( workdir ):
    # TODO     # Check to see first if the destination exists:
    # TODO     # print "XXX: %s -> %s" % (filename, filename.split())
    # TODO     # Split the absolute filename into a path and file pair:
    # TODO     # Use the same filename added to the destination path
    # TODO     tgtfile = os.path.join( dest, filename )
    # TODO     if os.path.isfile(tgtfile):
    # TODO         try:
    # TODO             os.remove(tgtfile)
    # TODO         except:
    # TODO             logger.error( "Weird error: found the file [%s] but can't remove it. The copy might fail." % tgtfile )
    # TODO     print "Copying %s -> %s." % (filename, dest)
    # TODO     copy( filename, dest )
    print "================================================================================"
    print "cycle_analyze.py - DONE."
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
    parser.set_defaults( logfile = "cycle_analyze.log",
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
