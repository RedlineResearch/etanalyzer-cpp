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
from mypytools import mean, stdev, variance, generalised_sum, median
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
        elif type(item) == set:
            newrow.append( singletons_to_str(item) )
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

def pair_sum( first = None,
              second = None ):
    return (first[0] + second[0], first[1] + second[1])

def output_cycle_summary_to_csv( typetup = {},
                                 # agerec_list = [],
                                 countrec_list = [],
                                 cpair_rec = {},
                                 cycle_writer = None,
                                 bmark = None,
                                 logger = None ):
    assert( len(typetup) > 0 )
    assert( type(typetup) == tuple )
    # assert( len(agerec_list) > 0 )
    assert( len(countrec_list) > 0 )
    assert( cycle_writer != None )
    assert( bmark != None )
    print "[%s TUP] - %s " % (bmark, typetup)
    # TODO #--------------------------------------------------------------------------------
    # TODO # AGEREC summary:
    # TODO range_list = []
    # TODO agepair_list = []
    # TODO for rec in agerec_list:
    # TODO     range_list.append( rec["range"] )
    # TODO     agepair_list.append( (rec["mean"], rec["groupcount"]) )
    # TODO age_count = 0
    # TODO age_total = 0.0
    # TODO age_total, age_count = reduce( pair_sum, agepair_list )
    # TODO age_mean = age_total / age_count
    # TODO age_range_mean = mean( range_list )
    #--------------------------------------------------------------------------------
    # OBJECT COUNT
    #--------------------------------------------------------------------------------
    if len(countrec_list) > 0:
        gcount_list = []
        singles = set()
        gsize_total = 0
        for rec in countrec_list:
            gcount_list.append( rec["groupcount"] )
            singles = set.intersection( singles, rec["singles"] )
            gsize_total += rec["groupsize"]
        smallest_cycle = min( gcount_list )
        largest_cycle = max( gcount_list )
        newrow = encode_row([ typetup,
                              len(countrec_list),
                              gsize_total, # bytes total
                              smallest_cycle, # object count minimum
                              largest_cycle, # object count maximum
                              mean(gcount_list), # object count mean
                              median(gcount_list), # object count median
                              singles, ] ) # singleton set
        try:
            cycle_writer.writerow(newrow)
        except:
            print "%d : %s = %s" % (str(typetup), type(key_nonjlib_alloc_site))
            print "Encoded row:"
            pp.pprint(newrow)
            exit(100)
    else:
        print "ERROR: %s has length 0." % str(typetup)

def new_cycle_age_record( new_min = None,
                          new_max = None,
                          age_range = None,
                          age_mean = None,
                          groupcount = None ):
    assert( age_mean != None ) # TODO DEBUG ONLY
    return { "min" : new_min,
             "max" : new_max,
             "range" : age_range,
             "mean" : age_mean,
             "groupcount" : groupcount }

def new_count_record( new_groupsize = None,
                      new_count = None,
                      new_singles = set() ):
    return { "groupsize" : new_groupsize,
             "groupcount" : new_count,
             "singles" : new_singles, }


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
                              atend_flag = False,
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
    #    Process
    #---------------------------------------------------------------------------
    print "--------------------------------------------------------------------------------"
    keytype_counter = Counter()
    deathsite_summary = defaultdict( Counter )
    dss = deathsite_summary # alias
    cycle_summary = Counter() # Keys are type tuples
    cycle_age_summary = defaultdict( lambda: [] )
    cycle_cpair_summary = defaultdict( lambda: [] )
    cycle_count_summary = defaultdict( lambda: [] )
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

        # RAW cycle file
        raw_cycle_header = [ "type-tuple", "cycles-size",
                             "obj-count",
                             "mean-age", "range-age",
                             "singletons", ]
        raw_cycle_writer = csv.writer( raw_cycfp,
                                       quoting = csv.QUOTE_NONNUMERIC )
        raw_cycle_writer.writerow( raw_cycle_header )
        # Cycle summary file
        cycle_header = [ "type-tuple", "cycle-count", "cycles-size",
                         "obj-count-min", "obj-count-max",
                         "obj-count-mean", "obj-count-median",
                         "singletons" ]
        cycle_writer = csv.writer( cycfp,
                                   quoting = csv.QUOTE_NONNUMERIC )
        cycle_writer.writerow( cycle_header )
        print "AT_END_FLAG:", atend_flag
        for gnum, glist in dgroups_data["group2list"].iteritems():
            # - for every death group dg:
            #       get the last edge for every object
            # TODO: Use the select parameter to choose which algorithm to use. TODO
            cyclelist, total_size, died_at_end_size, cause = get_cycles( group = glist,
                                                                         atend_flag = atend_flag,
                                                                         seen_objects = seen_objects,
                                                                         edgeinfo = edgeinfo,
                                                                         objectinfo = objectinfo,
                                                                         cycle_summary = cycle_summary,
                                                                         cycle_age_summary = cycle_age_summary,
                                                                         cycle_cpair_summary = cycle_cpair_summary,
                                                                         cycle_count_summary = cycle_count_summary,
                                                                         raw_cycle_writer = raw_cycle_writer,
                                                                         logger = logger )
            if cause == "END":
                assert( died_at_end_size > 0 )
                total_died_at_end_size += died_at_end_size
            elif len(cyclelist) > 0:
                assert( (cause == "HEAP") or (cause == "STACK") )
                total_alloc += total_size
        print "Age summary total = ", len(cycle_age_summary)
        print "Count summary total = ", len(cycle_count_summary)
        for typetup, countrec_list in cycle_count_summary.iteritems():
            # TODO # Get record from cycle_age_summary
            # TODO if typetup in cycle_age_summary:
            # TODO     agerec_list = cycle_age_summary[typetup]
            # TODO else:
            # TODO     agerec_list = []
            # TODO     agerec_list.append( new_cycle_age_record( new_min = 0,
            # TODO                                               new_max = 0,
            # TODO                                               age_range = 0,
            # TODO                                               age_mean = 0.0,
            # TODO                                               groupcount = 0 ) )
            # TODO TODO
            # Summary of key types
            output_cycle_summary_to_csv( typetup = typetup,
                                         # agerec_list = agerec_list,
                                         countrec_list = countrec_list,
                                         # cpair_rec = {}, # TODO TODO TODO HERE
                                         cycle_writer = cycle_writer,
                                         bmark = bmark,
                                         logger = logger )
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
                         edgeinfo = None,
                         flag = False ):
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
        if src not in nxG:
            nxG.add_node(src)
        if tgt not in nxG:
            nxG.add_node(tgt)
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
        age_list = [ 0, ]
    new_min = min(age_list)
    new_max = max(age_list)
    age_range = new_max - new_min
    age_mean = mean(age_list)
    return new_cycle_age_record( new_min = new_min,
                                 new_max = new_max,
                                 age_range = age_range,
                                 age_mean = age_mean,
                                 groupcount = len(age_list) )

def singletons_to_str( singletons = set() ):
    singfield = "(".encode('utf-8')
    for mytype in singletons:
        singfield += mytype.encode('utf-8')
        singfield += "|".encode('utf-8')
    singfield += ")".encode('utf-8')
    return singfield

def update_cycle_summary( cycle_summary = {},
                          cycledict = {},
                          cyclelist = [],
                          objectinfo = {},
                          cycle_age_summary = {},
                          cycle_cpair_summary = {},
                          cycle_count_summary = {},
                          raw_cycle_writer = None,
                          logger = None ):
    oi = objectinfo # Just a rename
    for nlist in cyclelist:
        typelist = []
        groupsize = 0
        for node in nlist:
            cycledict[node] = True
            mytype = oi.get_type(node)
            groupsize += oi.get_size(node)
            typelist.append( mytype )
        typecount = Counter(typelist)
        typelist = list( set(typelist) )
        assert( len(typelist) > 0 ) # TODO. Only a debugging assert
        # Use a default order to prevent duplicates in tuples
        tup = tuple( sorted( typelist ) )
        # Save count
        cycle_summary[tup] += 1
        # Get the death site
        dsite = get_cycle_deathsite( nlist,
                                     objectinfo = oi )
        #--------------------------------------------------------------------------------
        age_rec = get_cycle_age_stats( cycle = nlist,
                                       objectinfo = oi )
        #--------------------------------------------------------------------------------
        singletons = set()
        for mytype, value in typecount.iteritems():
            if value == 1:
                singletons.add( mytype )
        count_rec = new_count_record( new_groupsize = groupsize,
                                      new_count = age_rec["groupcount"],
                                      new_singles = singletons )
        #--------------------------------------------------------------------------------
        if (count_rec != None):
            # TODO: How about checking for age_rec too? TODO
            # cycle_age_summary:
            #    key: type tuple
            #    value: list of cycle age summary records
            cycle_age_summary[tup].append( age_rec  )
            # cycle_count_summary:
            #    key: type tuple
            #    value: list of cycle count summary records
            cycle_count_summary[tup].append( count_rec  )
            # TODO Death site, allocation site TODO
            singfield = singletons_to_str( singletons )
            if age_rec != None:
                age_mean = age_rec["mean"]
                age_range = age_rec["range"]
            else:
                age_mean = 0.0
                age_range = 0
            raw_cycle_writer.writerow( encode_row([ tup,
                                                    groupsize,
                                                    len(nlist),
                                                    age_mean,
                                                    age_range,
                                                    singfield, ]) )
        else:
            logger.error( "TODO: Unable to save cycle %s" % str(cyclelist) )

def my_find_cycle():
    # SIMPLE CYCLES FIRST
    cycles_gen = nx.simple_cycles(nxgraph)
    cycles_list = list(cycles_gen)
    #    Add to cycles_list:
    for mycycle in cycles_list:
        new_cycle = []
        for nxnode in mycycle:
            objsize = objectinfo.get_size(nxnode)
            groupsize += objsize
            cycle_nodes.add(nxnode)
            assert(nxnode in cycledict)
            cycledict[nxnode] = True
            new_cycle.append( nxnode )
            seen.add( nxnode ) # Save in seen set
        if len(new_cycle) > 0:
            cyclelist.append( new_cycle )
        # else do a DEBUG? Shouldn't have an empty cycle here. TODO
    # NON-SIMPLE CYCLES NEXT
    # count = 1
    # for scclist in nx.strongly_connected_components(nxgraph):
    #     print "SCC %d: %d" % (count, len(scclist))
    #     count += 1
    # print "NODES:", nxgraph.number_of_nodes()
    # print "EDGES:", nxgraph.number_of_edges()
    for node in nxgraph.nodes_iter():
        try:
            print "FIND_CYCLE:",
            edge_list = nx.find_cycle( nxgraph, node )
            print " done: %d" % len(edge_list)
            new_cycle = []
            for edge in edge_list:
                # print "---:", edge, type(edge)
                for nxnode in edge:
                    if nxnode in seen:
                        break
                    objsize = objectinfo.get_size(nxnode)
                    groupsize += objsize
                    cycle_nodes.add(nxnode)
                    assert(nxnode in cycledict)
                    cycledict[nxnode] = True
                    new_cycle.append( nxnode )
                    # seen_objects.add( nxnode ) # Save in seen_objects
            if len(new_cycle) > 0:
                new_cycle = list(set(new_cycle))
                cyclelist.append( new_cycle )
                set.update( new_cycle )
                print "  -- cycle len: %d" % len(new_cycle)
        except:
            print " exception."
            # DEBUG only: print "Object[ %d ]: No cycle found." % node
            pass # NOOP

def is_self_loop( nxgraph = None,
                  node = None ):
    slist = nxgraph.successors( node )
    for x in slist:
        if x == node:
            return True
    return False

class Algo:
    USE_DTIME = 101
    USE_ESTATE = 102
    USE_APPROX = 103
# This should return a dictionary where:
#     key -> group (list INCLUDES key)
def get_cycles( group = {},
                atend_flag = False,
                select = Algo.USE_ESTATE,
                seen_objects = set(),
                edgeinfo = None,
                objectinfo = None,
                cycle_summary = {},
                cycle_age_summary = {},
                cycle_cpair_summary = {},
                cycle_count_summary = {},
                raw_cycle_writer = None,
                logger = None ):
    latest = 0 # Time of most recent
    tgt = 0
    assert( len(group) > 0 )
    dtime = objectinfo.get_death_time( group[0] )
    flag = False
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
    should_process = lambda x: ((x == "END") if atend_flag else (x != "END"))
    assert( cause != None and
            ( cause == "HEAP" or
              cause == "STACK" or
              cause == "END" ) ) # These are the only valid final states.
    # Sum up the size in bytes
    # Go look for the cycles if we are told to:
    if ( (len(group) == 1) and
         should_process(cause) ):
        print "XXX: %s" % cause
        # sys.stdout.write( "SIZE 1: %d --" % len(group) )
        # Group of 1 and it didn't die at the end.
        obj = group[0]
        if cause == "END":
            died_at_end_size += objectinfo.get_size(obj)
        else:
            pass # TODO TODO TODO TODO
        if obj not in seen_objects:
            # Sum up the size in bytes
            objsize = objectinfo.get_size(obj)
            total_size += objsize
            # Get all edges that target 'obj':
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            if select == Algo.USE_DTIME:
                edgelist = [ x for x in srcreclist if
                             (ei.get_death_time_from_rec(x) == dtime) ]
            elif select == Algo.USE_ESTATE:
                estate = "BY_OBJECT_DEATH" if not atend_flag \
                    else "BY_PROGRAM_END"
                edgelist = [ x for x in srcreclist if
                             (ei.get_edgestate_from_rec(x) == estate) ]
            elif select == Algo.USE_APPROX:
                assert( False )
                # TODO TODO TODO TODO
                edgelist = [ x for x in srcreclist if
                             (ei.get_death_time_from_rec(x) == dtime) ]
            else:
                assert( False )
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
                                              cycle_count_summary = cycle_count_summary,
                                              logger = logger )
                        break
        return ( cyclelist,
                 total_size, # size of group in bytes
                 died_at_end_size,
                 cause ) # death cause
    elif should_process(cause):
        assert( len(group) > 1 )
        print "XXX: %s" % cause
        edgelist = []
        groupsize = 0
        for obj in group:
            if cause == "END":
                died_at_end_size += objectinfo.get_size(obj)
            else:
                pass # TODO TODO TODO TODO
            # Sum up the size in bytes
            objsize = objectinfo.get_size(obj)
            total_size += objsize
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            if select == Algo.USE_DTIME:
                obj_edgelist = [ x for x in srcreclist if
                                 (ei.get_death_time_from_rec(x) == dtime) ]
            elif select == Algo.USE_ESTATE:
                estate = "BY_OBJECT_DEATH" if not atend_flag \
                    else "BY_PROGRAM_END"
                obj_edgelist = [ x for x in srcreclist if
                                 (ei.get_edgestate_from_rec(x) == estate) ]
            elif select == Algo.USE_APPROX:
                assert( False )
                # TODO TODO TODO TODO
                obj_edgelist = [ x for x in srcreclist if
                                 (ei.get_death_time_from_rec(x) == dtime) ]
                # TODO TODO TODO TODO
            else:
                assert( False )
            if len(obj_edgelist) > 0:
                edgelist.extend( obj_edgelist )
            # if flag:
            #     print "LENGTH srcreclist: %d" % len(srcreclist)
            #     print "       obj_edgelist: %d" % len(obj_edgelist)
            #     print "   NEW edgelist: %d" % len(edgelist)
        # TODO: if flag:
        # TODO:     print "***FINAL LENGTH obj_edgelist: %d" % len(edgelist)
        # TODO:     for tmp in edgelist:
        # TODO:         print "XXX:", tmp
        graph_result = make_adjacency_list( nodes = group,
                                            edgelist = edgelist,
                                            edgeinfo = ei )
        adjlist = graph_result["adjlist"]
        nxgraph = graph_result["nxgraph"]
        cycle_nodes = set()
        cycledict = { n : False for n in nxgraph.nodes() }
        seen = set() # save the seen objects here since we need to do
        #              a DFS on every object
        # TODO
        if ( (select == Algo.USE_DTIME) or
             (select == Algo.USE_ESTATE) ):
            # TODO: Do I need a selector for which cycle function to call?
            # Use networkx's cycle_basis
            scclist = nx.strongly_connected_components( nxgraph )
            scclist = list(scclist)
            cycle = []
            for scc in scclist:
                scc = list(scc)
                if ( (len(scc) > 1) or
                     (is_self_loop(nxgraph, scc[0])) ):
                    cycle = scc
            if len(cycle) > 0:
                cyclelist.append( cycle )
        elif select == Algo.USE_APPROX:
            assert(False) # NOT TO BE USED
            # for node in group:
            #     refcount = objectinfo.get_refcount(node)
            #     if refcount > 0:
            #         cycle.append(node)
        else:
            assert( False )
        if len(cyclelist) > 0:
            update_cycle_summary( cycle_summary = cycle_summary,
                                  cycledict = cycledict,
                                  cyclelist = cyclelist,
                                  objectinfo = objectinfo,
                                  cycle_age_summary = cycle_age_summary,
                                  raw_cycle_writer = raw_cycle_writer,
                                  cycle_cpair_summary = cycle_cpair_summary,
                                  cycle_count_summary = cycle_count_summary,
                                  logger = logger )
        return ( cyclelist,
                 total_size,
                 died_at_end_size,
                 cause )
    else:
        assert( not should_process(cause) )
        # Ignore anything that we want to ignore.
        for obj in group:
            if obj in seen_objects:
                # Dupe. TODO: Log a warning/error
                continue
            if cause == "END":
                died_at_end_size += objectinfo.get_size(obj)
            else:
                pass # TODO TODO TODO TODO
        return (cyclelist, total_size, died_at_end_size, cause)

# Conveniently save the version I finished for the Onward revision.
# 17 July 2017 - Raoul V.
# This should return a dictionary where:
#     key -> group (list INCLUDES key)
def get_cycles_revised_ONWARD( group = {},
                               seen_objects = set(),
                               edgeinfo = None,
                               objectinfo = None,
                               cycle_summary = {},
                               cycle_age_summary = {},
                               cycle_cpair_summary = {},
                               cycle_count_summary = {},
                               raw_cycle_writer = None,
                               logger = None ):
    latest = 0 # Time of most recent
    tgt = 0
    assert( len(group) > 0 )
    dtime = objectinfo.get_death_time( group[0] )
    flag = False
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
        if flag:
            print "A"
        obj = group[0]
        if obj not in seen_objects:
            if flag:
                print "B"
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
                if flag:
                    print "C"
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
                                              cycle_count_summary = cycle_count_summary,
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
            # * Incoming edges
            srcreclist = ei.get_sources_records(obj)
            # We only want the ones that died with the object
            # TODO what is this for? TODO: ei.get_source_id_from_rec(x)
            obj_edgelist = [ x for x in srcreclist if
                             (ei.get_death_time_from_rec(x) == dtime) ]
            # if flag:
            #     print "LENGTH srcreclist: %d" % len(srcreclist)
            #     print "       obj_edgelist: %d" % len(obj_edgelist)
            if len(obj_edgelist) > 0:
                edgelist.extend( obj_edgelist )
            # if flag:
            #     print "LENGTH srcreclist: %d" % len(srcreclist)
            #     print "       obj_edgelist: %d" % len(obj_edgelist)
            #     print "   NEW edgelist: %d" % len(edgelist)
        # TODO: if flag:
        # TODO:     print "***FINAL LENGTH obj_edgelist: %d" % len(edgelist)
        # TODO:     for tmp in edgelist:
        # TODO:         print "XXX:", tmp
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
        seen = set() # save the seen objects here since we need to do
        #              a DFS on every object
        # TODO
        cycle = []
        for node in group:
            refcount = objectinfo.get_refcount(node)
            if refcount > 0:
                cycle.append(node)
        if len(cycle) > 0:
            cyclelist.append( cycle )
            # if len(cycle) > 3:
            #     print "XXX:", len(cycle)
        if len(cyclelist) > 0:
            update_cycle_summary( cycle_summary = cycle_summary,
                                  cycledict = cycledict,
                                  cyclelist = cyclelist,
                                  objectinfo = objectinfo,
                                  cycle_age_summary = cycle_age_summary,
                                  raw_cycle_writer = raw_cycle_writer,
                                  cycle_cpair_summary = cycle_cpair_summary,
                                  cycle_count_summary = cycle_count_summary,
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
                  atend_flag = False,
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
    # START
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
                                  atend_flag,
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
                                      atend_flag = atend_flag,
                                      cycle_result = cycle_summary_all[bmark],
                                      logger = logger )
            # TODO: while not results[bmark].empty():
            # TODO:     row = results[bmark].get()
            # TODO:     key_summary_writer.writerow( row )
            # TODO: key_summary_fp.flush()
            # TODO: os.fsync(key_summary_fp.fileno())

    #-----[ Checking if we need to poll ]-------------------------------------------
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
                    # TODO: while not results[bmark].empty():
                    # TODO:     row = results[bmark].get()
                    # TODO:     key_summary_writer.writerow( row )
                    # TODO: key_summary_fp.flush()
                    # TODO: os.fsync(key_summary_fp.fileno())
                    timenow = time.asctime()
                    logger.debug( "[%s] - done at %s" % (bmark, timenow) )
        print "======[ Processes DONE ]========================================================"
        sys.stdout.flush()
        # TODO: Need to output cycle information
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
    parser.add_argument( "--at-end-only",
                         dest = "atend_flag",
                         help = "Analyze only cycles that died at program's end.",
                         action = "store_true" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "cycle_analyze.log",
                         debugflag = False,
                         config = None,
                         atend_flag = False )
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
                         atend_flag = args.atend_flag,
                         logger = logger )

if __name__ == "__main__":
    main()
