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
from multiprocessing import Process, Manager

# Possible useful libraries, classes and functions:
# from operator import itemgetter
#   - This one is my own library:
from mypytools import mean, stdev, variance, check_host

# The garbology related library. Import as follows.
# Check garbology.py for other imports
from garbology import ObjectInfoReader, StabilityReader, ReferenceReader, \
         ReverseRefReader, DeathGroupsReader, SummaryReader, get_index, is_stable

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
def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None

def get_objects_from_stable_group( sgnum = 0,
                                   stable_grouplist = []):
    return stable_grouplist[sgnum].nodes() if (sgnum < len(stable_grouplist)) else []

#================================================================================
#================================================================================
def output_graph_and_summary( bmark = "",
                              objreader = {},
                              dgraph = {},
                              dgraph_unstable = {},
                              wcclist = [],
                              wcclist_unstable = {},
                              stable_grouplist = [],
                              stable2deathset = {},
                              death2stableset = {},
                              backupdir = None,
                              logger = None ):
    # Print to standard output
    print "=======[ SUMMARY ]=============================================================="
    print "[%s] -> # of objects = %d" % (bmark, len(objreader))
    # The first stable graph
    print "=======[ STABLE GRAPH ]========================================================="
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
    # The second unstable graph
    print "=======[ UNSTABLE GRAPH ]======================================================="
    print "     -> supernodes = %d  edges = %d  - super WCC = %d" % \
        ( dgraph_unstable.number_of_nodes(),
          dgraph_unstable.number_of_edges(),
          len(wcclist_unstable) )
    print "     -> 3 largest super WCC     = %d, %d, %d" % \
        ( len(wcclist_unstable[0]), len(wcclist_unstable[1]), len(wcclist_unstable[2]) )
    wcc_num_objects = []
    for sgnum in [0, 1, 2]:
        objlist = get_objects_from_stable_group( sgnum, stable_grouplist )
        wcc_num_objects.append(objlist)
    print "     ->    in number of objects = %d, %d, %d" % \
            ( len(wcc_num_objects[0]), len(wcc_num_objects[1]), len(wcc_num_objects[2]) )
    target = "%s-UNstable_graph.gml" % bmark
    # Backup the old gml file if it exists
    if os.path.isfile(target):
        # Move this file into backup directory
        bakfile = os.path.join( backupdir, target )
        if os.path.isfile( bakfile ):
            os.remove( bakfile )
        shutil.move( target, backupdir )
    nx.write_gml(dgraph_unstable, target)
        
def read_simulator_data( bmark = "",
                         cycle_cpp_dir = "",
                         objectinfo_config = {},
                         dgroup_config = {},
                         stability_config = {},
                         reference_config = {},
                         reverse_ref_config = {},
                         summary_config = {},
                         mydict = {},
                         logger = None ):
    # Read in OBJECTINFO
    print "Reading in the OBJECTINFO file for benchmark:", bmark
    sys.stdout.flush()
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
    # Read in CYCLES (which contains the death groups)
    print "Reading in the CYCLES (deathgroup) file for benchmark:", bmark
    sys.stdout.flush()
    mydict["dgroupreader"] = DeathGroupsReader( os.path.join( cycle_cpp_dir,
                                                              dgroup_config[bmark] ),
                                                logger = logger )
    dgroupreader = mydict["dgroupreader"]
    try:
        dgroupreader.read_dgroup_file( objreader )
    except:
        logger.error( "[ %s ] Unable to read cycles (deathgroup) file.." % bmark )
        mydict.clear()
        sys.stdout.flush()
        return False
    # Read in STABILITY
    print "Reading in the STABILITY file for benchmark:", bmark
    sys.stdout.flush()
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
    sys.stdout.flush()
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
    sys.stdout.flush()
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
    # Read in SUMMARY
    print "Reading in the SUMMARY file for benchmark:", bmark
    sys.stdout.flush()
    mydict["summary_reader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                     summary_config[bmark] ),
                                       logger = logger )
    try:
        summary_reader = mydict["summary_reader"]
        summary_reader.read_summary_file()
    except:
        logger.error( "[ %s ] Unable to read summary file.." % bmark )
        mydict.clear()
        sys.stdout.flush()
        return False
    sys.stdout.flush()
    return True

def debug_None_death_group( sobjId = None,
                            counter = {},
                            objreader = {} ):
    if sobjId not in objreader:
        counter["not_found"] += 1
    else:
        if objreader.died_by_program_end(sobjId):
            counter["died_by_end"] += 1
        elif objreader.died_by_stack(sobjId):
            counter["died_by_stack"] += 1
        elif objreader.died_by_heap(sobjId):
            counter["died_by_heap"] += 1
        elif objreader.died_by_global(sobjId):
            counter["died_by_global"] += 1

def create_stable_death_bipartite_graph( stable2deathset = {},
                                         death2stableset = {},
                                         DAE_groupnum = None,
                                         logger = None ):
    # DAE_groupnum is the 'died at end' group number
    assert(DAE_groupnum != None) # TODO: More checking needed?
    digraph = nx.Graph()
    skeys = stable2deathset.keys()
    dkeys = death2stableset.keys()
    for sgroup in skeys:
        digraph.add_node( "S%d" % sgroup )
    for dgroup in dkeys:
        if dgroup != DAE_groupnum:
            digraph.add_node( "D%d" % dgroup )
    done_edge = set()
    for sgroup in skeys:
        dset = stable2deathset[sgroup]
        for dtgt in dset:
            if dtgt == DAE_groupnum:
                continue
            digraph.add_edge( "S%d" % sgroup, "D%d" % dtgt )
            done_edge.add( ("S%d" % sgroup, "D%d" % dtgt) )
    for dgroup in dkeys:
        if dgroup == DAE_groupnum:
            continue
        stable_set = death2stableset[dgroup]
        for stgt in stable_set:
            if (stgt, dgroup) not in done_edge:
                digraph.add_edge( "D%d" % dgroup, "S%d" % stgt )
                # When adding the edge, it really doesn't matter
                # which way it goes.  But the done_edge set means
                # that we use the stable group node first.
                done_edge.add( ("S%d" % stgt, "D%d" % dgroup) )
    return digraph

def get_objects_as_set( stable_list = [],
                        death_list = [],
                        stable_grouplist = [],
                        dgroup_reader = {},
                        objreader = {},
                        sd_combined_id = None ):
    objset = set()
    for sgnum in stable_list:
        for objId in stable_grouplist[sgnum]:
            objset.add(objId)
            objreader.set_stable_group_number(objId, sgnum)
            objreader.set_combined_sd_group_number(objId, sd_combined_id)
    sgnum = None # To prevent dynamic PL problems
    for dgnum in death_list:
        for objId in dgroup_reader.get_group(dgnum):
            objset.add(objId)
            objreader.set_death_group_number(objId, dgnum)
            objreader.set_combined_sd_group_number(objId, sd_combined_id)
    return objset

def output_each_object( objset = set(),
                        seen_objects = set(),
                        writer = None,
                        stable = {},
                        death = {},
                        objreader = {},
                        bmark = "",
                        logger = None ):
    for objId in objset:
        if objId in seen_objects:
            logger.error( "[%s] Duplicate object - %d" % (bmark, objId) )
            continue
        seen_objects.add(objId)
        stable_gnum = objreader.get_stable_group_number(objId)
        death_gnum = objreader.get_death_group_number(objId)
        combined_gnum = objreader.get_comibined_sd_group_number(objId)
        # object Id, stable group number, death group number, combined stable+death group number,
        #        allocation time, death time
        writer.writerow( [ objId,
                           stable_gnum, death_gnum, combined_gnum,
                           objreader.get_alloc_time(objId),
                           objreader.get_death_time(objId), ] )


def summarize_wcc_stable_death_components( wcc_sd_list = [],
                                           stable_grouplist = [],
                                           objreader = {} ,
                                           dgroup_reader = {},
                                           summary_reader = {},
                                           bmark = "",
                                           output_filename = None,
                                           logger = None ):
    summary = defaultdict(dict)
    # Summary is indexed by JOINT stable/death group number
    for index in xrange(len(wcc_sd_list)):
        graph = wcc_sd_list[index]
        # The lists for 'stable' and 'death' contain objectIds.
        summary[index]["stable"] = []
        summary[index]["death"] = []
        summary[index]["objects"] = set()
        # Shorten the names
        stable = summary[index]["stable"]
        death = summary[index]["death"]
        # Node ids in the graph are of the form of:
        #     D123   or S456
        # where D = death group
        #       S = stable group
        for node in graph.nodes():
            gtype = node[:1]
            gnum = int(node[1:])
            if gtype == "S": # Stable group
                stable.append(gnum)
            elif gtype == "D": # Death group
                death.append(gnum)
            else:
                raise ValueError( "Unexpected node type: %s for %s" % (gtype, node) )
        summary[index]["objects"] = get_objects_as_set( stable_list = stable,
                                                        death_list = death,
                                                        stable_grouplist = stable_grouplist,
                                                        dgroup_reader = dgroup_reader,
                                                        objreader = objreader,
                                                        sd_combined_id = index )
    to_number = 5 if len(summary) > 5 else len(summary)
    assert(to_number > 0)
    # Get the final time for the benchmark
    final_time = summary_reader.get_final_garbology_time()
    assert(final_time > 0)
    with open(output_filename, "wb") as fptr:
        seen_objects = set()
        # Create the CSV writer and write the header row
        writer = csv.writer( fptr, quoting = csv.QUOTE_NONNUMERIC )
        writer.writerow( [ "objectId", "stable group number", "death group number",
                           "combined group number", "allocation time", "death time", ] )
        for index in xrange(to_number):
            # Rename into shorter names
            stable = summary[index]["stable"]
            death = summary[index]["death"]
            objset = summary[index]["objects"]
            # Output the per object row in the CSV
            output_each_object( objset = objset,
                                seen_objects = seen_objects,
                                writer = writer,
                                stable = stable,
                                death = death,
                                objreader = objreader,
                                bmark = bmark,
                                logger = logger )
            #------------------------------------------------------------
            # Get total number of objects
            summary[index]["total_objects"] = len(objset)
            alloc_time_list = [ objreader.get_alloc_time(x) for x in objset ]
            alloc_time_set = set(alloc_time_list)
            death_time_list = [ objreader.get_death_time(x) for x in objset ]
            death_time_set = set(death_time_list)
            summary[index]["number_alloc_times"] = len(alloc_time_set)
            summary[index]["number_death_times"] = len(death_time_set)
            #------------------------------------------------------------
            # In the following the _sc suffix (as in X_sc) means it's scaled
            # to the total time in percentage.
            # 1. Get minimum-maximum alloc times
            #      * Alloc time range
            min_alloctime = min( alloc_time_list )
            max_alloctime = max( alloc_time_list )
            summary[index]["atime"] = { "min" : min_alloctime,
                                        "min_sc" : (min_alloctime / final_time) * 100.0, 
                                        "max" : max_alloctime,
                                        "max_sc" : (max_alloctime / final_time) * 100.0}
            #      * Death time range
            min_deathtime = min( death_time_list )
            max_deathtime = max( death_time_list )
            summary[index]["dtime"] = { "min" : min_deathtime,
                                        "min_sc" : (min_deathtime / final_time) * 100.0, 
                                        "max" : max_deathtime,
                                        "max_sc" : (max_deathtime / final_time) * 100.0}
            #  Alloc and Death time statistics
            mean_deathtime = mean( death_time_list  )
            stdev_deathtime = stdev( death_time_list  )
            mean_alloctime = mean( alloc_time_list  )
            stdev_alloctime = stdev( alloc_time_list  )
            #     Allocation time
            summary[index]["atime"]["mean"] = mean_alloctime
            summary[index]["atime"]["stdev"] = stdev_alloctime
            #     Death time
            summary[index]["dtime"]["mean"] = mean_deathtime
            summary[index]["dtime"]["stdev"] = stdev_deathtime
            # The scaled (_sc) quantities
            #     Allocation time
            summary[index]["atime"]["mean_sc"] = (mean_alloctime / final_time) * 100.0
            summary[index]["atime"]["stdev_sc"] = (stdev_alloctime / final_time) * 100.0
            #     Death time
            summary[index]["dtime"]["mean_sc"] = (mean_deathtime / final_time) * 100.0
            summary[index]["dtime"]["stdev_sc"] = (stdev_deathtime / final_time) * 100.0
            #------------------------------------------------------------
            # 2. Get minimum-maximum death times
            #     - std deviation? median?
            #     - categorize according to death groups? or stable groups?
            #          or does it matter?
            # 3. Get total drag
    print "======[ %s ][ SUMMARY of components ]===========================================" % bmark
    for index in sorted(summary.keys()):
        if index > to_number:
            break
        mydict = summary[index]
        print "Component %d" % index
        for key, val in mydict.iteritems():
            if key == "total_objects":
                print "    * %d objects" % val
            elif key == "number_alloc_times":
                print "    * %d unique allocation times" % val
            elif key == "number_death_times":
                print "    * %d unique death times" % val
            elif key == "atime":
                print "    * alloc range - [ {:.2f}, {:.2f} ]".format(val["min_sc"], val["max_sc"])
                print "    * mean = {:.2f}    stdev = {:.2f}".format(val["mean_sc"], val["stdev_sc"])
            elif key == "dtime":
                print "    * death range - [ {:.2f}, {:.2f} ]".format(val["min_sc"], val["max_sc"])
                print "    * mean = {:.2f}    stdev = {:.2f}".format(val["mean_sc"], val["stdev_sc"])
    print "======[ %s ][ END SUMMARY of components ]=======================================" % bmark
    return

#--------------------------------------------------------------------------------
# Super graph ONE related functions
def add_nodes_to_graph( objreader = {},
                        objnode_list = set(),
                        logger = None ):
    dgraph = nx.DiGraph()
    TYPE = get_index( "TYPE" ) # type index
    for tup in objreader.iterrecs():
        objId, rec = tup
        mytype = objreader.get_type_using_typeId( rec[TYPE] )
        if objId not in objnode_list:
            dgraph.add_node( objId, { "type" : mytype } )
            objnode_list.add( objId )
        else:
            logger.critical( "%s: Multiple add for object Id [ %s ]" %
                             (bmark, str(objId)) )
    return dgraph

def add_stable_edges( dgraph = {},
                      stability = {},
                      reference = {},
                      objnode_list = {},
                      logger = None ):
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

#--------------------------------------------------------------------------------
# Super graph ONE related functions
def add_nodes_to_graph_TWO( stable_grouplist = [],
                            stnode_list = set(),
                            logger = None ):
    # TODO Do we need stnode_list? After all, we have all the stnodes here now
    #      in stable_grouplist.
    dgraph = nx.DiGraph()
    for sgnum in xrange(len(stable_grouplist)):
        objlist = stable_grouplist[sgnum]
        if sgnum not in stnode_list:
            dgraph.add_node( sgnum ) # TODO: What attributes do we add?
            stnode_list.add( sgnum )
        else:
            logger.critical( "%s: Multiple add for stable group [ %s ]" %
                             (bmark, str(stgnum)) )
    return dgraph

def add_unstable_edges( dgraph = {},
                        stability = {},
                        reference = {},
                        obj2stablegroup = {},
                        stable_grouplist = [],
                        stnode_list = set(),
                        objnode_list = {},
                        logger = None ):
    edgeset = set()
    for sgnum in xrange(len(stable_grouplist)): # for each stable group number
        if sgnum not in stnode_list:
            print "=========[ ERROR ]=============================================================="
            pp.pprint( objnode_list )
            print "=========[ ERROR ]=============================================================="
            print "ObjId [ %s ] of type [ %s ]" % (str(objId), str(type(objId)))
            assert(False)
        objlist = stable_grouplist[sgnum]
        for objId in objlist: # for each field Id of each object
            fdict = stability[objId]
            if fdict == None:
                continue
            for fieldId, sattr in fdict.iteritems(): # for each field Id of each object
                if not is_stable(sattr):
                    # Get the target object Ids
                    try:
                        tgt_objlist = reference[ (objId, fieldId) ]
                    except:
                        print "ERROR: Not found (%s, %s)" % (str(objId), str(fieldId))
                        logger.error("ERROR: Not found (%s, %s)" % (str(objId), str(fieldId)))
                        print "EXITING."
                        exit(10)
                    missing = set([])
                    for tgtObjId in tgt_objlist: # for each target object
                        # Look for the stable group number for tgtObjId
                        tgt_sgnum = obj2stablegroup[tgtObjId]
                        edge = (sgnum, tgt_sgnum)
                        if edge in edgeset:
                            continue
                        dgraph.add_edge( edge[0], edge[1] )
                        edgeset.add( edge )
                    # HERE
    return dgraph

# Go through the stable weakly-connected list and find the death groups
# that the objects are in.
def map_stable_to_deathgroups( stable_grouplist = [], # input
                               atend_gnum = {}, # input
                               dgreader = {}, # input
                               stable2deathset = {}, # output
                               objId_seen = {}, # output
                               obj2stablegroup = {}, # output
                               logger = None ):
    # stable_grouplist - stable group list of object Ids
    # atend_gnum - the stable group number for programs that 'died at end'
    # dgreader - DeathGroupReader for reading in death group data from simulator
    # stable2deathset - map stable group to related death group
    # objId_seen - remember all the objects we've seen
    # obj2stablegroup - map object Id to the corresponding stable group
    for stable_groupId in xrange(len(stable_grouplist)):
        dgroups = set()
        for sobjId in stable_grouplist[stable_groupId]:
            # Get the death group number from the dgroup reader
            dgroupId = dgreader.get_group_number(sobjId)
            assert(dgroupId != None)
            # Save the death group Id
            dgroups.add(dgroupId)
            # Add to seen objects set
            objId_seen.add(sobjId)
            # Update the reverse mapping of 
            if sobjId not in obj2stablegroup:
                obj2stablegroup[sobjId] = stable_groupId
            else:
                logger.critical( "Multiple stable groups for object Id [ %s ] -> %d | %d" %
                                 (str(objId), stable_groupId, obj2stablegroup[sobjId]) )
                print "ERROR: Multiple stable groups for object Id [ %s ] -> %d | %d" % \
                                 (str(objId), stable_groupId, obj2stablegroup[sobjId])
                assert(False) # For now. TODO TODO TODO
                obj2stablegroup[sobjId] = stable_groupId
        # Save in a list, then print out.
        # IDEA: A bipartite graph???
        #       Connected component makes for GC region???
        # DEBUG: keeping this here just in case
        # if len(dgroups) > 0:
        #     sys.stdout.write( "[ Stable group %d ]: %s\n" % (stable_groupId, str(dgroups)) )
        # DEBUG END
        stable2deathset[stable_groupId].update( dgroups )

def map_death_to_stablegroups( stable2deathset = {},
                               dgreader = {},
                               objId_seen = {},
                               obj2stablegroup = {},
                               death2stableset = {},
                               logger = None ):
    # stable2deathset - map stable group to related death group
    # dgreader - DeathGroupReader for reading in death group data from simulator
    # objId_seen - remember all the objects we've seen
    # obj2stablegroup - map object Id to the corresponding stable group
    # death2stableset - map death group to related stable group
    not_seen = 0
    for sgroupId, dgset in stable2deathset.iteritems():
        for dgroupId in dgset:
            # The relationship is symmetric:
            death2stableset[dgroupId].add(sgroupId)
            for objId in dgreader.get_group(dgroupId):
                # Are there any objects in our death groups that haven't been mapped?
                if objId not in objId_seen:
                    # Note that this isn't expected.
                    not_seen += 1
                    objId_seen.add(sobjId)
                    dgroupId = dgreader.get_group_number(objId)
                    new_sgroupId = obj2stablegroup[objId]
                    death2stableset[dgroupId].add( new_sgroupId )
    return not_seen

def create_supergraph_all_MPR( bmark = "",
                               cycle_cpp_dir = "",
                               main_config = {},
                               objectinfo_config = {},
                               dgroup_config = {},
                               stability_config = {},
                               reference_config = {},
                               reverse_ref_config = {},
                               summary_config = {},
                               result = [],
                               logger = None ):
    # Assumes that we are in the desired working directory.
    # Get all the objects and add as a node to the graph
    mydict = {}
    backupdir = main_config["backup"]
    # Read all the data in.
    read_result = read_simulator_data( bmark = bmark,
                                       cycle_cpp_dir = cycle_cpp_dir,
                                       objectinfo_config = objectinfo_config,
                                       dgroup_config = dgroup_config,
                                       stability_config = stability_config,
                                       reference_config = reference_config,
                                       reverse_ref_config = reverse_ref_config,
                                       summary_config = summary_config,
                                       mydict = mydict,
                                       # shared_list = result,
                                       logger = logger )
    if read_result == False:
        return False
    # Extract the important reader objects
    objreader = mydict["objreader"]
    stability = mydict["stability"]
    reference = mydict["reference"]
    summary_reader = mydict["summary_reader"]
    # Get the type index
    TYPE = get_index( "TYPE" )
    # Start the graph by adding nodes
    objnode_list =  set([])
    # Add nodes to graph
    dgraph = add_nodes_to_graph( objreader = objreader,
                                 objnode_list = objnode_list,
                                 logger = logger )
    # Add the stable edges only
    add_stable_edges( dgraph = dgraph,
                      stability = stability,
                      reference = reference,
                      objnode_list = objnode_list,
                      logger = logger )
    # Get the weakly connected components
    wcclist = sorted( nx.weakly_connected_component_subgraphs(dgraph),
                      key = len,
                      reverse = True )
    #---------------------------------------------------------------------------
    # Here's where the stable groups are compared against the death groups
    # 1 - For every stable group, determine the deathgroup number set
    # 2 - For every death group, determine the stable group number set
    # Using the order of the sorted wcclist, let group 1 be at index 0 and so on.
    # Therefore this means that the largest wcc is at index 0, and is called group 1.
    dgreader = mydict["dgroupreader"]
    stable2dgroup = {}
    s2d = stable2dgroup # Make it shorter
    counter = Counter()
    # Maintain the results in 2 dictionaries:
    # 'stable2deathset' which maps:
    #     stable group num -> set of death group numbers
    stable2deathset = defaultdict(set)
    # 'death2stableset' which maps:
    #     death group number -> set of stable group numbers
    death2stableset = defaultdict(set)
    # As a sanity check, we keep track of which object Ids we've seen:
    objId_seen = set()
    obj2stablegroup = {}
    # Get the group number for 'died at end' group since we want to ignore that group
    atend_gnum = dgreader.get_atend_group_number()
    # Rename wcclist to a more appropriate name. stable_grouplist is a list of
    #     object Ids. We use the list indices as a stable group number mapping to the 
    #     object Ids.
    stable_grouplist = wcclist
    #---------------------------------------------------------------------------
    # Go through the stable weakly-connected list and find the death groups
    # that the objects are in.
    map_stable_to_deathgroups( stable_grouplist = stable_grouplist, # input
                               atend_gnum = atend_gnum, # input
                               dgreader = dgreader, # input
                               stable2deathset = stable2deathset, # output
                               objId_seen = objId_seen, # output
                               obj2stablegroup = obj2stablegroup, # output
                               logger = logger )

    # Do a reverse mapping from death group to stable
    not_seen = map_death_to_stablegroups( stable2deathset = stable2deathset, # input
                                          dgreader = dgreader, # input
                                          objId_seen = objId_seen, # input/output
                                          obj2stablegroup = obj2stablegroup, # input
                                          death2stableset = death2stableset, # output
                                          logger = logger )
    logger.error( "NOT SEEN: %d" % not_seen )
    # Make a bipartite stable <-> death group graph
    stable_death_graph = create_stable_death_bipartite_graph( stable2deathset = stable2deathset,
                                                              death2stableset = death2stableset,
                                                              DAE_groupnum = atend_gnum,
                                                              logger = logger )
    wcc_stable_death_list = sorted( nx.connected_component_subgraphs(stable_death_graph),
                                    key = len,
                                    reverse = True )
    summarize_wcc_stable_death_components( wcc_sd_list = wcc_stable_death_list,
                                           stable_grouplist = stable_grouplist,
                                           objreader = objreader,
                                           dgroup_reader = dgreader,
                                           summary_reader = summary_reader,
                                           bmark = bmark,
                                           output_filename = os.path.join( main_config["output"], 
                                                                           "%s-stabledeath-object-summary.csv" % bmark ),
                                           logger = logger,
                                           )
    print "============[ %s :: Stable <-> Death graph ]=======================================" % bmark
    print "[%s] Number of nodes: %d" % (bmark, stable_death_graph.number_of_nodes())
    print "[%s] Number of edges: %d" % (bmark, stable_death_graph.number_of_edges())
    print "[%s] Number of components: %d" % (bmark, len(wcc_stable_death_list))
    print "[%s] Top 5 largest components: %s" % (bmark, str( [ len(x) for x in wcc_stable_death_list[:5] ] ))
    print "================================================================================"
    print "================================================================================"
    #---------------------------------------------------------------------------
    # Create the next super graph TWO. Which consists of stable groups + unstable edges.
    # - Create new graph using stable groups as nodes.
    #       Start the graph by adding nodes
    stnode_list =  set([])
    # Add nodes to graph
    dgraph_unstable = add_nodes_to_graph_TWO( stable_grouplist = stable_grouplist,
                                              stnode_list = stnode_list,
                                              logger = logger )
    # - Add the UNSTABLE edges.
    add_unstable_edges( dgraph = dgraph_unstable,
                        stability = stability,
                        reference = reference,
                        stable_grouplist = stable_grouplist,
                        obj2stablegroup = obj2stablegroup,
                        stnode_list = stnode_list,
                        logger = logger )
    # Get the weakly connected components
    wcclist_unstable = sorted( nx.weakly_connected_component_subgraphs(dgraph_unstable),
                               key = len,
                               reverse = True )
    #---------------------------------------------------------------------------
    output_graph_and_summary( bmark = bmark,
                              objreader = objreader,
                              dgraph = dgraph,
                              dgraph_unstable = dgraph_unstable,
                              wcclist = wcclist,
                              wcclist_unstable = wcclist_unstable,
                              stable_grouplist = stable_grouplist,
                              backupdir = backupdir,
                              stable2deathset = stable2deathset,
                              death2stableset = death2stableset,
                              logger = logger )
    result.append( { "graph" : dgraph,
                     "graph_unstable" : dgraph_unstable,
                     "wcclist" : wcclist,
                     "stable2deathset" : stable2deathset,
                     "death2stableset" : death2stableset } )

def main_process( global_config = {},
                  objectinfo_config = {},
                  dgroup_config = {},
                  host_config = {},
                  worklist_config = {},
                  main_config = {},
                  reference_config = {},
                  reverse_ref_config = {},
                  stability_config = {},
                  summary_config = {},
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
    supergraph = {}
    manager = Manager()
    procs = {}
    results = {}
    for bmark in worklist_config.keys():
        # TODO START
        hostlist = worklist_config[bmark]
        if not check_host( benchmark = bmark,
                           hostlist = hostlist,
                           host_config = host_config ):
            continue
        # Else we can run for 'bmark'
        if mprflag:
            print "=======[ Spawning %s ]================================================" \
                % bmark
            results[bmark] = manager.list([ bmark, ])
            p = Process( target = create_supergraph_all_MPR,
                         args = ( bmark,
                                  cycle_cpp_dir,
                                  main_config,
                                  objectinfo_config,
                                  dgroup_config,
                                  stability_config,
                                  reference_config,
                                  reverse_ref_config,
                                  summary_config,
                                  results[bmark],
                                  logger ) )
            procs[bmark] = p
            p.start()
        else:
            wcclist = create_supergraph_all( bmark = bmark,
                                             cycle_cpp_dir = cycle_cpp_dir,
                                             main_config = main_config,
                                             objectinfo_config = objectinfo_config,
                                             dgroup_config = dgroup_config,
                                             stability_config = stability_config,
                                             reference_config = reference_config,
                                             reverse_ref_config = reverse_ref_config,
                                             logger = logger )
    if mprflag:
        # Poll the processes 
        done = False
        expected = len(procs.keys())
        numdone = 0
        for bmark in procs.keys():
            procs[bmark].join()
        for bmark in procs.keys():
            proc = procs[bmark]
            done = False
            while not done:
                done = True
                if proc.is_alive():
                    done = False
                else:
                    numdone += 1
                    print "==============================> [%s] DONE." % bmark
                    del procs[bmark]
            sys.stdout.flush()
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
    host_config = config_section_map( "hosts", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    dgroup_config = config_section_map( "etanalyze-output", config_parser )
    worklist_config = config_section_map( "create-supergraph-worklist", config_parser )
    reference_config = config_section_map( "reference", config_parser )
    reverse_ref_config = config_section_map( "reverse-reference", config_parser )
    stability_config = config_section_map( "stability-summary", config_parser )
    summary_config = config_section_map( "summary-cpp", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    return { "global" : global_config,
             "main" : main_config,
             "objectinfo" : objectinfo_config,
             "dgroup" : dgroup_config,
             "hosts" : host_config,
             "create-supergraph-worklist" : worklist_config,
             "reference" : reference_config,
             "reverse-reference" : reverse_ref_config,
             "stability" : stability_config,
             "summary_config" : summary_config,
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
    host_config = process_host_config( configdict["hosts"] )
    main_config = configdict["main"]
    objectinfo_config = configdict["objectinfo"]
    dgroup_config = configdict["dgroup"]
    reference_config = configdict["reference"]
    reverse_ref_config = configdict["reverse-reference"]
    stability_config = configdict["stability"]
    summary_config = configdict["summary_config"]
    worklist_config = process_worklist_config( configdict["create-supergraph-worklist"] )
    # pp.pprint(worklist_config)
    # PROBABLY DELETE:
    # contextcount_config = configdict["contextcount"]
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
                         dgroup_config = dgroup_config,
                         host_config = host_config,
                         worklist_config = worklist_config,
                         reference_config = reference_config,
                         reverse_ref_config = reverse_ref_config,
                         stability_config = stability_config,
                         summary_config = summary_config,
                         logger = logger )

if __name__ == "__main__":
    main()
