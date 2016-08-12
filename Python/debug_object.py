# debug_object.py 
#
import csv
import os
import sys
import time
import logging
import argparse

import pprint

# from collections import Counter
# from traceback import print_stack
# from operator import itemgetter


from etparse import is_valid_op, is_heap_alloc_op, is_heap_op, parse_line, \
                    is_method_op, heap_entry_fields
from strace import STrace, StatClass
from mypytools import get_file_fp
# Ref counting not needed
# TODO REF from sumrefcount_detailed import *

pp = pprint.PrettyPrinter( indent = 4 )

GB = 1099511627776 # 1 gigabyte

def setup_logger( targetdir = ".",
                  filename = "debug_object.log",
                  logger_name = "debug_object",
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

def create_record( objId = None,
                   mytype = None,
                   rectype = None ):
    return { "objId" : objId,
             "type" : mytype,
             "rectype" : rectype }

def inc_count( heap, objId ):
    heap[objId]["rc"] += 1

def dec_count( heap, objId, tracepoint ):
    global negrc_dict
    # TODO TODO TODO TODO
    # if objId not in negrc_dict:
    heap[objId]["rc"] -= 1
    if heap[objId]["rc"] < 0:
        if objId not in negrc_dict:
            negrc_dict[objId] = 1
        else:
            negrc_dict[objId] += 1
        print "Negative RC: %d = %d : %s" % (objId, negrc_dict[objId], str(heap[objId]["root"]))

#
# Heap Data structure
# hash: objId -> tuple
#    tuple is:
#        if A: (threadId, type, size)
#           D: NONE
#           U: (threadId, newTgtId, oldTgtId)
def populate_heap_record( heap = None,
                          rec = None,
                          color = None,
                          # TODO REF summary = None,
                          time_by_method = None,
                          live = False,
                          logger = None ):
    global pp
    global type_map

    if rec != None:
        objId = rec["objId"]
        if rec["rectype"] in my_alloc_types:
            heap[objId]["f"] = []
            heap[objId]["at"] = int(time_by_method)
            heap[objId]["dt"] = None
            heap[objId]["t"] = rec["type"]
            heap[objId]["atype"] = rec["rectype"]
            heap[objId]["col"] = color
            heap[objId]["rc"] = 0
            heap[objId]["live"] = live
            heap[objId]["root"] = False
            # # DEBUG
            # heap[objId]["hist"] = []
            # TODO REF summary.alloc_rec( objId )
        else:
            logger.error( "invalid rec type: %s  -(expecting an A or N)" % rec["rectype"] )
            raise RuntimeError()
    else:
        logger.error( "Invalid record. %s" % pp.pformat(rec) )
        raise ValueError( "Invalid record." )

def update_fields( objrec = None,
                   newId = None ):
    assert( newId != None )
    objrec["f"].append( newId )

def remove_from_fields( objrec = None,
                        tgtId = None ):
    if tgtId in objrec["f"]:
        objrec["f"].remove( tgtId )


def recolor( rec, color, heap ):
    # recolor is from [2]
    assert( color != BLACK )
    for tgt in rec["f"]:
        if tgt in heap:
            if ( (rec["col"] == GREEN or rec["col"] == BLACK) and
                 (color != GREEN and color != BLACK) ):
                heap[tgt]["rc"] -= 1
            elif ( (rec["col"] != GREEN and rec["col"] != BLACK) and
                   (color == GREEN or color == BLACK) ):
                heap[tgt]["rc"] += 1
    rec["col"] = color

def recolor_lazy( rec, color, heap ):
    # recolor_lazy is from [2]
    for tgt in rec["f"]:
        if tgt in heap:
            if ( (rec["col"] == GREEN or rec["col"] == BLACK) and
                 (color != GREEN and color != BLACK) ):
                heap[tgt]["rc"] -= 1
            elif ( (rec["col"] != GREEN and rec["col"] != BLACK) and
                   (color == GREEN or color == BLACK) ):
                heap[tgt]["rc"] += 1
    rec["col"] = color

def mark_red( objId, heap ):
    # mark_red as described in [2]
    #    - checks if objId is in heap
    if objId not in heap:
        return
    # objId in heap
    rec = heap[objId]
    # try:
    if rec["col"] == GREEN:
        recolor_lazy( rec, RED, heap )
        for tgt in rec["f"]:
            mark_red( tgt, heap )

def mark_red_lazy( objId, heap ):
    # mark_red_lazy as described in [2]
    #    - checks if objId is in heap
    if objId not in heap:
        return
    # objId in heap
    rec = heap[objId]
    # try:
    if rec["col"] == GREEN or rec["col"] == BLACK:
        recolor_lazy( rec, RED, heap )
        for tgt in rec["f"]:
            mark_red_lazy( tgt, heap )

def scan_green( objId, heap ):
    if objId not in heap:
        return
    rec = heap[objId]
    recolor_lazy( rec, GREEN, heap )
    for tgt in rec["f"]:
        if tgt not in heap:
            # TODO: Should we log these?
            continue
        if heap[tgt]["col"] != GREEN:
            scan_green( tgt, heap )

def scan( objId, heap ):
    if objId not in heap:
        return
    rec = heap[objId]
    if rec["col"] == RED:
        if rec["rc"] > 0:
            scan_green( objId, heap )
        else:
            recolor_lazy( rec, BLUE, heap )
            for tgt in rec["f"]:
                scan( tgt, heap )

def collect_blue( objId, heap ):
    if objId not in heap:
        return
    rec = heap[objId]
    retlist = []
    if rec["col"] == BLUE:
        recolor_lazy( rec, GREEN, heap )
        for tgt in rec["f"]:
            retlist = collect_blue( tgt, heap )
        retlist.append( objId )
    return retlist

def delete_edge( srcId = None,
                 tgtId = None,
                 heap = None,
                 recursive_decrement = False ):
    # Update "fields"
    if srcId != 0 and srcId in heap:
        remove_from_fields( objrec = heap[srcId],
                            tgtId = tgtId )
    # Ignore objId of 0 as these are static/global (and not heap) references.
    if tgtId != 0:
        dec_count( heap, tgtId, "A" )
        # TODO TODO TODO
        if heap[tgtId]["rc"] == 0:
            for newtgt in heap[tgtId]["f"]:
                # print "%s -> %s" % (str(srcId), str(newtgt))
                if newtgt == 0:
                    continue
                dec_count( heap, newtgt, "B" )
                if recursive_decrement and newtgt in heap:
                    if heap[newtgt]["rc"] == 0:
                        for tmptgt in heap[newtgt]["f"]:
                            if tmptgt in heap:
                                delete_edge( srcId = newtgt,
                                             tgtId = tmptgt,
                                             heap = heap,
                                             recursive_decrement = True )
        else:
            pass

def delete_edge_02( srcId = None,
                    tgtId = None,
                    heap = None,
                    recursive_decrement = False ):
    global noncycle_count, cycle_count
    global cyclelist
    # Update "fields"
    if srcId != 0 and srcId in heap:
        remove_from_fields( objrec = heap[srcId],
                            tgtId = tgtId )
    # Ignore objId of 0 as these are static/global (and not heap) references.
    if tgtId != 0:
        if heap[tgtId]["rc"] == 1:
            dec_count( heap, tgtId, "A" )
            for newtgt in heap[tgtId]["f"]:
                # print "%s -> %s" % (str(srcId), str(newtgt))
                if newtgt == 0:
                    continue
                delete_edge_02( srcId = tgtId,
                                tgtId = newtgt,
                                heap = heap,
                                recursive_decrement = recursive_decrement )
        else:
            dec_count( heap, tgtId, "B" )
            mark_red( tgtId, heap )
            scan( tgtId, heap )
            cycle = collect_blue( tgtId, heap )
            if len(cycle) > 0:
                # print "X: %s" % str(cycle)
                cycle_count += 1
                cyclelist.append( cycle )
            else:
                noncycle_count += 1

def delete_edge_02_lazy( srcId = None,
                         tgtId = None,
                         heap = None,
                         recursive_decrement = False ):
    global nodeset
    # As described in [2]
    # Update "fields"
    if srcId != 0 and srcId in heap:
        remove_from_fields( objrec = heap[srcId],
                            tgtId = tgtId )
    # Ignore objId of 0 as these are static/global (and not heap) references.
    if tgtId != 0:
        rec = heap[tgtId]
        if rec["rc"] == 1:
            dec_count( heap, tgtId, "A" )
            # TODO Should this be recolor?
            rec["col"] = GREEN
            for newtgt in rec["f"]:
                # print "%s -> %s" % (str(srcId), str(newtgt))
                if newtgt == 0:
                    continue
                delete_edge_02_lazy( srcId = tgtId,
                                     tgtId = newtgt,
                                     heap = heap,
                                     recursive_decrement = recursive_decrement )
        else:
            dec_count( heap, tgtId, "B" )
            if rec["col"] != BLACK:
                recolor_lazy( rec, BLACK, heap )
                nodeset.update( [ tgtId ] )

def scan_queue( heap ):
    # As described in [2]
    global noncycle_count, cycle_count
    global nodeset
    clist = []
    for tgtId in nodeset:
        rec = heap[tgtId]
        if rec["col"] == BLACK:
            mark_red_lazy( tgtId, heap )
            scan( tgtId, heap )
            cycle = collect_blue( tgtId, heap )
            if len(cycle) > 0:
                # print "X: %s" % str(cycle)
                cycle_count += 1
                clist.append( cycle )
            else:
                noncycle_count += 1
    return clist

def update_heap_record( heap = None,
                        rec = None,
                        recObjId = None,
                        # TODO REF summary = None,
                        time_by_method = None,
                        recursive_decrement = False,
                        logger = None ):
    global nodeset
    global pp
    if rec != None:
        objId = rec["objId"]
        oldTgtId = rec["oldTgtId"]
        newTgtId = rec["newTgtId"]
        if objId not in heap:
            logger.error( "objId[ %s ] not found in heap." % objId )
            assert( objId in heap )
        if newTgtId not in heap:
            # If not in heap, create a fake record so that we can use the update.
            logger.warning( "newTgtId[ %s ] not found in heap." % newTgtId )
            newrec = create_record( objId = newTgtId,
                                    rectype = "A" )
            heap[newTgtId] = {}
            # TODO: problem here for our local mark-scan cycle detection algorithm
            # What color should this be? TODO
            populate_heap_record( heap = heap,
                                  rec = newrec,
                                  # TODO REF summary = summary,
                                  time_by_method = time_by_method,
                                  live = True,
                                  logger = logger )
        # TODO: This is the DELETE operation
        if oldTgtId != 0:
            delete_edge_02_lazy( srcId = objId,
                                 tgtId = oldTgtId,
                                 heap = heap, 
                                 recursive_decrement = recursive_decrement )
        if newTgtId in nodeset:
            nodeset.remove( newTgtId )
        if heap[newTgtId] == BLACK:
            recolor_lazy( rec, GREEN, heap )
        update_fields( objrec = heap[objId],
                       newId = newTgtId )
        # # DEBUG
        # if oldTgtId != 0:
        #     heap[oldTgtId]["hist"].append(rec)
        if newTgtId != 0:
            inc_count( heap, newTgtId )
            # # DEBUG
            # heap[newTgtId]["hist"].append(rec)
    else:
        logger.error( "invalid record." )
        raise exceptions.ValueError( "rec can not be 'None'" )
    return

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

def counter_to_list( counter ):
    global pp
    countlist = [ (key, val) for (key, val) in counter.iteritems() ]
    countlist.sort( key = itemgetter(0) )
    return countlist

def save_refcounts_to_csv( ref_output = None,
                           counter = None,
                           benchmark = None ):
    counter_list = counter_to_list( counter )
    with open( ref_output, "wb" ) as fpcsv:
        refwriter = csv.writer( fpcsv )
        refwriter.writerow( [ "benchmark", "refcount", "total" ] )
        for line in counter_list:
            refwriter.writerow( [ benchmark, line[0], line[1] ] )

def pickle_all( heap = None,
                # TODO REF summary = None,
                outpickle = None,
                ref_output = None,
                benchmark = None,
                store = None,
                logger = None,
                gml_filename = None ):
    global pp, Stats
    global cyclelist
    print "=========[ TRACE SUMMARY ]===================================================="
    print "R events:", Stats.revents
    print "Allocations:", Stats.allocates
    print "\tAdded twice:", Stats.addtwice
    print "Updates:", Stats.updates
    print "\tUpdate nomatch:", Stats.update_nomatch
    print "Deaths:", Stats.deaths
    print "\tDeath nomatch:", Stats.death_nomatch
    print "Thread match:", Stats.thread_match
    print "\tThread nomatch:", Stats.thread_nomatch
    print "Method entries:", Stats.methentry
    print "Method exits:", Stats.methexit
    print "Method match:", Stats.method_match
    print "\tMethod nomatch:", Stats.method_nomatch
    print "No allocation site:", Stats.no_allocsite
    print "OK receiver object ID:", Stats.found_recobjid
    print "\tNo receiver object ID:", Stats.no_recobjid
    print "=========[ REFCOUNT SUMMARY ]================================================="
    counter = Counter( [ rec["rc"] for rec in heap.itervalues() if not rec["live"] ] )
    pp.pprint(counter)
    save_refcounts_to_csv( ref_output = ref_output,
                           counter = counter,
                           benchmark = benchmark )
    print "=========[ NO GRAPH SUMMARY ]================================================="
    print "=========[ SAVE TO PICKLE ]==================================================="
    # Things to pickle:
    logger.debug( "Attempting to pickle to [%s]:", outpickle )
    try:
        pfile = open( outpickle, 'wb' )
    except:
        logger.error( "Unable to open pickle file: %s", outpickle )
        exit(41)
    if pfile != None:
        cPickle.dump( cyclelist, pfile )

# TODO: Is this called? 15 October 2013
def add_to_typedict( typedict = None,
                     heap = None,
                     rec = None ):
    global logger, pp
    raise RuntimeError( "add_to_typedict should not be called." )
    op = rec["rectype"]
    if op == "A" and rec["type"] != None:
        if rec["type"] not in typedict:
            typedict[rec["type"]] = set()
    elif op == "U":
        objId = rec["objId"]
        if objId == 0:
            return
        utype = heap[rec["objId"]]["type"] if "type" in heap[rec["objId"]] else None
        newTgtId = rec["newTgtId"]
        oldTgtId = rec["oldTgtId"]
        if utype != None and utype in typedict:
            try:
                tgttype = heap[newTgtId]["type"]
            except:
                print "[%s] not found in heap." % rec["newTgtId"]
                print "rec:"
                pp.pprint( rec )
                print "heap:"
                pp.pprint( heap )
                exit(7)
            if oldTgtId == 0:
                # update from NULL
                typedict[utype].add( newTgtId )
            else:
                try:
                    typedict[utype].remove( oldTgtId )
                except:
                    print "TODO: [%s] not found in typedict." % oldTgtId
                finally:
                    typedict[utype].add( newTgtId )
        else:
            # What can we do here?
            # We have an update to an object whose type we don't know.
            # TODO TODO
            # Log a warning here?
            pass
    else:
        # TODO What else should be addressed here?
        pass

Stats = StatClass()

def get_methodId( strace = None,
                  threadId = None ):
    return strace.get_current_methId( threadId )

def debug_process_heap_event( heap = None,
                              rec = None,
                              typedict = None,
                              strace = None ):
                              # TODO REF summary = None ):
    global logger
    logger.error( "rec[ %s ] strace[ %d ]" %
                  (str(rec), strace.get_number_of_threads()) )

def process_heap_event( heap = None,
                        deadhash = None,
                        ignored_alloc = None,
                        rec = None,
                        strace = None,
                        store = None,
                        # TODO REF summary = None,
                        time_by_method = None,
                        recursive_decrement = False,
                        logger = None ):
    global Stats
    # pdb.set_trace() # TODO Debug only
    op = rec["rectype"]
    objId = rec["objId"]
    if "threadId" in rec:
        threadId = rec["threadId"]
    else:
        threadId = None
    if op in my_alloc_types:
        if not rec["objId"] in heap:
            heap[objId] = {}
            populate_heap_record( heap = heap,
                                  rec = rec,
                                  # TODO REF summary = summary,
                                  time_by_method = time_by_method,
                                  color = GREEN,
                                  live = True,
                                  logger = logger )
            # DEBUG
            logger.debug( "Object ADDED objId[%s]" % str(objId) )
            Stats.allocates += 1
        else:
            logger.warning( "[%s] ADDED but already IN heap.", str(objId) )
            Stats.addtwice += 1
        # # DEBUG
        # heap[objId]["hist"].append(rec)
    elif op == "I" or op == "P":
        ignored_alloc.update( [ objId ] )
    elif op == "U":
        Stats.updates += 1
        if objId == '0' or objId in ignored_alloc:
            # Update to object is 0 is ignored.
            # Update to alloc types I and P are also ignored via 'ignored_alloc'
            pass
        else:
            # 2013-11-10 Notes:
            # UPDATE: We do care about connectivity. How can we recursively apply 
            # the reference decrement if we don't have connectivity information.
            # IDEA: Maybe we should keep it in the summary instead of a separate heap?
            # IDEA: Even if the object hasn't seen an ALLOC record, the edge is important.
            if objId not in heap:
                if objId not in deadhash:
                    # Update to an object not in the heap and whose death record hasn't
                    # been seen yet.
                    logger.warning( "WARNING: updated object NOT FOUND [%s]", str(objId) )
                    newrec = create_record( objId = objId,
                                            mytype = "None",
                                            rectype = "A" )
                    heap[objId] = {}
                    populate_heap_record( heap = heap,
                                          rec = newrec,
                                          # TODO REF summary = summary,
                                          color = GREEN,
                                          live = True,
                                          logger = logger,
                                          time_by_method = 0 ) # time is at 0 since we don't really know.
                    update_heap_record( heap = heap,
                                        rec = rec,
                                        # TODO REF summary = summary,
                                        time_by_method = time_by_method,
                                        recursive_decrement = recursive_decrement,
                                        logger = logger )
                    # TODO: Update the increment counts.
                    # - This should be an increment for one and a decrement for the old target.
                else:
                    logger.error( "Update after death objId[ %s ] type[ %s ], alloctype[ %s ]"
                                  " refcount[ %d ]" %
                                  (objId, deadhash[objId][0], deadhash[objId][1], deadhash[objId][2] ) )
                                   # TODO REF summary[objId]["refcount"]) )
                    logger.error( "REC: %s" % pp.pformat(rec) )
                    # What was the idea here?
                    # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
                    # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
                    # Fix Stats updates. TODO
                    Stats.set_update_after_death_with_info( objId = objId,
                                                            mytype = deadhash[objId][0],
                                                            alloctype = deadhash[objId][1] )
                    # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
                    # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
            else:
                update_heap_record( heap = heap,
                                    rec = rec,
                                    # TODO REF summary = summary,
                                    time_by_method = time_by_method,
                                    recursive_decrement = recursive_decrement,
                                    logger = logger )
            # # DEBUG
            # heap[objId]["hist"].append(rec)
    elif op == "D":
        if objId not in heap or not heap[objId]["live"]:
            if objId not in ignored_alloc and objId not in deadhash:
                logger.warning( "Dying object NOT FOUND [%s]", str(objId) )
                Stats.death_nomatch += 1
            elif objId in deadhash:
                logger.warning( "Multiple death object [%s]", str(objId) )
                Stats.multiple_death += 1
        else:
            tmprec = heap[objId]
            tmprec["deathtime"] = int(time_by_method)
            deadhash[objId] = (tmprec["t"], tmprec["atype"], tmprec["rc"]) 
            heap[objId]["live"] = False
            # If heap object is a ROOT object, delete_edge_02_lazy from object 0
            if heap[objId]["root"]:
                delete_edge_02_lazy( srcId = 0,
                                     tgtId = objId,
                                     heap = heap,
                                     recursive_decrement = recursive_decrement )
                heap[objId]["root"] = False
            # if heap[objId]["root"]:
            #     heap[objId]["rc"] -= 1
            # # DEBUG
            # heap[objId]["hist"].append(rec)
            logger.debug( "Object REMOVED [%s]", str(objId) )
            Stats.deaths += 1
    elif op == "M":
        # Method entry
        time_by_method = time_by_method + 1
        strace.enter_method( threadId = rec["threadId"],
                             methId = rec["methodId"],
                             recObjId = objId )
        Stats.methentry += 1
    elif op == "E":
        # Method exit
        time_by_method = time_by_method + 1
        tid = rec["threadId"]
        methId = rec["methodId"]
        strace.exit_method( threadId = tid,
                            methId = methId,
                            recObjId = objId )
        Stats.methexit += 1
    elif op == "R":
        if (objId in heap) and (not heap[objId]["root"]):
            inc_count( heap, objId )
            heap[objId]["root"] = True
            # # DEBUG
            # heap[objId]["hist"].append(rec)
        Stats.revents = Stats.revents + 1
    else:
        logger.debug( "Event [%s] not handled. TODO.", str(op) )
    return time_by_method

def dump_summary( summary = None ):
    mypp = pprint.PrettyPrinter( indent = 4 )
    print "TODO"

def process_input( myfp = None,
                   tgtId = None,
                   logger = None ):
    heap = {}
    deadhash = {}
    ignored_alloc = set([])
    strace = STrace( Stats = Stats )

    cur = 0
    time_by_methup = 0
    time_by_alloc = 0
    for x in myfp:
        rec = parse_line( line = x,
                          logger = logger )
        cur = cur + 1
        op = rec["rectype"]
        if is_method_op(op):
            time_by_methup += 1
        elif is_heap_alloc_op(op):
            time_by_alloc += rec["size"]
        for field in [ "objId", "newTgtId", "oldTgtId", ]:
            if (field in rec) and (tgtId == rec[field]):
                print "[%s]: %s" % (op, str(rec))

        
    print "======================================================================"


def main_process( tgtpath = None,
                  tgtId = None,
                  debugflag = False,
                  logger = None ):
    fp = get_file_fp( myfile = tgtpath,
                      logger = logger )
    process_input( myfp = fp,
                   tgtId = tgtId,
                   logger = logger )
    print "=====[ DONE ]========================================"
    logger.error( "=====[ DONE ]========================================" )
    # Just a debugging output. Make sure it's logged by marking it critical.
    logger.critical( "###: %s" % time.strftime("%c") )
    fp.close()

def setup_options( parser = None ):
    parser.add_argument( "tracefile",
                          help = "Set name of raw input Elephant Tracks file" )
    parser.add_argument( "objId",
                          help = "Object ID to debug" )
    parser.add_argument( "--logfile",
                          help = "filename to use for log file" )
    # set logging to debug level
    parser.add_argument( "--debug",
                         dest = "debugflag",
                         action = "store_true",
                         help = "Enable debug mode." )
    parser.set_defaults( logfile = "debug_object.log",
                         debugflag = False )

def main():
    global pickleflag
    # initialize path variables
    # process options
    usage = "usage: %prog [options]"
    # parser = OptionParser( usage=usage )
    parser = argparse.ArgumentParser()
    setup_options( parser )
    args = parser.parse_args()
    #
    # Get input filename
    tgtpath = args.tracefile
    try:
        if not os.path.exists( tgtpath ):
            parser.error( tgtpath + " does not exist." )
    except:
        parser.error( "invalid path name : " + tgtpath )
    
    # Get object Id
    objId = args.objId
    try:
        objId = int(objId)
    except:
        parser.error( "Invalid object Id: %s" % str(objId) )
    # Actually open the input db/file in main_process()
    # 
    # Get logfile
    logfile = args.logfile if args.logfile != None else \
              "debug_object-" + os.path.basename(tgtpath) + ".log"
    # set debug flag
    debugflag = args.debugflag
    logger_name = "debug_object"
    logger = setup_logger( filename = logfile,
                           logger_name = logger_name,
                           debugflag = debugflag )
    #
    # Main processing
    #
    return main_process( tgtpath = tgtpath,
                         tgtId = objId,
                         debugflag = debugflag,
                         logger = logger )

if __name__ == "__main__":
    main()
