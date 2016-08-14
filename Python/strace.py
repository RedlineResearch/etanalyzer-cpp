from time import strftime
import logging
import logging.handlers
import os
import StringIO
from collections import Counter

class StatClass:
    def __init__( self ):
        self.methentry = 0
        self.methexit = 0
        self.method_match = 0
        self.method_nomatch = 0
        self.thread_match = 0
        self.thread_nomatch = 0
        self.revents = 0
        self.allocates = 0
        self.deaths = 0
        self.updates = 0
        self.death_nomatch = 0
        self.update_nomatch = 0
        self.multiple_death = 0
        self.addtwice = 0
        self.no_allocsite = 0
        self.no_recobjid = 0
        self.found_recobjid = 0
        self.call_tid_match = 0
        self.call_tid_nomatch = 0
        self.__update_after_death = 0
        self.update_objId = set([])
        self.update_count = Counter()
        self.lost_method = 0
        self.empty_callstack = 0

    @property
    def update_after_death( self ):
        return self.__update_after_death

    @update_after_death.setter
    def update_after_death( self, value ):
        self.__update_after_death = value

    def set_update_after_death_with_info( self,
                                          objId = None,
                                          mytype = None,
                                          alloctype = None ):
        self.update_after_death += 1
        if objId != None and mytype != None:
            if objId not in self.update_objId:
                self.update_count.update( [ (mytype, alloctype) ] )
    
    def get_summary_as_dict( self ):
        return { "summary" : { "methentry" : self.methentry,
                               "methexit" : self.methexit,
                               "method_match" : self.method_match,
                               "method_nomatch" : self.method_nomatch,
                               "thread_match" : self.thread_match,
                               "thread_nomatch" : self.thread_nomatch,
                               "revents" : self.revents,
                               "allocates" : self.allocates,
                               "deaths" : self.deaths,
                               "updates" : self.updates,
                               "death_nomatch" : self.death_nomatch,
                               "multiple_death" : self.multiple_death,
                               "update_nomatch" : self.update_nomatch,
                               "addtwice" : self.addtwice,
                               "no_allocsite" : self.no_allocsite,
                               "no_recobjid" : self.no_recobjid,
                               "found_recobjid" : self.found_recobjid,
                               "call_tid_match" : self.call_tid_match,
                               "call_tid_nomatch" : self.call_tid_nomatch,
                               "update_after_death" : self.update_after_death, },
                 "update after death object list" : list(self.update_count) }

class STrace:
    def __init__( self, logger = None, Stats = None ):
        self.strace = {}
        self.mylogger = None
        self.setup_logger()
        self.logger = logger if logger != None else self.mylogger
        self.Stats = Stats if Stats != None else StatClass()
        # for most recent method entry/exit
        self.remember_context( threadId = None,
                               methId = None,
                               methType = None )

    def remember_context( self,
                          threadId = None,
                          methId = None,
                          methType = None ):
        # TODO add DEPTH
        self.last_method = methType
        self.last_method_id = methId
        self.last_context_thread_id = threadId
        if threadId != None and self.has_thread(threadId):
            self.last_context = [ x[1] for x in self.strace[threadId] ]
        else:
            self.last_context = []

    def setup_logger( self ):
        timestr = strftime("%y%j%H%M")
        # Set up main logger
        self.mylogger = logging.getLogger( "strace-" + timestr )
        formatter = logging.Formatter( '[%(funcName)s] : %(message)s' )
        self.mylogger.setLevel( logging.DEBUG )
        filehandler = logging.FileHandler( os.path.join( "./strace-" + timestr + ".log" ) , 'w' )
        filehandler.setLevel( logging.DEBUG )
        filehandler.setFormatter( formatter )
        self.mylogger.addHandler( filehandler )

    def get_logger( self ):
        return self.mylogger

    def enter_method( self,
                      threadId = None,
                      methId = None,
                      recObjId = None ):
        self.Stats.methentry = self.Stats.methentry + 1
        if not self.has_thread( threadId ):
            self.strace[threadId] = []
        self.strace[threadId].append( (recObjId, methId) )
        # self.mylogger.debug( "Entry method %s[%s]" % (str(threadId), str(methId)) )

    def enter_method_and_remember( self,
                                   threadId = None,
                                   methId = None,
                                   recObjId = None ):
        self.enter_method( threadId = threadId,
                           methId = methId,
                           recObjId = recObjId )
        self.remember_context( threadId = threadId,
                               methId = methId,
                               methType = STrace.METHOD.ENTRY )

    def exit_method( self,
                     threadId = None,
                     methId = None,
                     recObjId = None ):
        self.Stats.methexit = self.Stats.methexit + 1
        if threadId in self.strace:
            self.Stats.thread_match = self.Stats.thread_match + 1
            try:
                lastrec = self.strace[threadId].pop()
            except:
                # empty list!
                # If you add anything to the try clause,
                # make sure to refine the except clause.
                self.Stats.empty_callstack = self.Stats.empty_callstack + 1
                self.logger.warning( "Exit method %s[ %s ] recId[ %s ] CAN'T BE MATCHED. Call stack empty." %
                                     (str(threadId), str(methId), str(recObjId)) )
                return None
            while 1:
                if len(self.strace[threadId]) == 0:
                    self.logger.error( "Exit method TID %s[ %s ] recId[ %s ] CAN'T BE MATCHED. Call stack empty." %
                                       (str(threadId), str(methId), str(recObjId)) )
                    break
                if methId != lastrec[1]:
                    self.logger.error( "Method ID mismatch [ %s ] in thread[%s]",
                                       methId, threadId )
                    self.Stats.method_nomatch = self.Stats.method_nomatch + 1
                    lastrec = self.strace[threadId].pop()
                    self.Stats.lost_method = self.Stats.lost_method + 1
                else:
                    self.Stats.method_match = self.Stats.method_match + 1
                    # self.mylogger.debug( "Exit method %s[%s]" % (str(threadId), str(methId)) )
                    break
        else:
            self.mylogger.error( "NOMATCH: No matching thread ID[%s] for method exit id[%s]" %
                                 (threadId, methId) )
            self.logger.error( "NOMATCH: No matching thread ID[%s] for method exit id[%s]" %
                               (threadId, methId) )
            self.Stats.thread_nomatch = self.Stats.thread_nomatch + 1

    def exit_method_and_remember( self,
                                  threadId = None,
                                  methId = None,
                                  recObjId = None ):
        self.remember_context( threadId = threadId,
                               methId = methId,
                               methType = STrace.METHOD.EXIT )
        self.exit_method( threadId = threadId,
                          methId = methId,
                          recObjId = recObjId )

    def has_thread( self, tid ):
        return tid in self.strace

    def get_current_recObjId( self, tid ):
        if self.has_thread(tid):
            if len(self.strace[tid]) > 0:
                self.Stats.found_recobjid = self.Stats.found_recobjid + 1
                return self.strace[tid][-1][0]
        self.Stats.no_recobjid = self.Stats.no_recobjid + 1
        return None
        # TODO Add stats for NONE vs found recObjId TODO
        # TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

    def get_current_methId( self, tid ):
        if self.has_thread(tid):
            if len(self.strace[tid]) > 0:
                return self.strace[tid][-1][1]
        return None
    
    def get_number_of_threads( self ):
        return len(self.strace)

    def get_stack( self, tid = None ):
        if self.has_thread(tid):
            return self.strace[tid]
        else:
            return None

    def get_calling_context( self,
                             threadId = None ):
        """
        Returns the calling context with most recent first in the list.
        """
        if threadId != None:
            tid = threadId # rename
            if self.has_thread(tid):
                self.Stats.call_tid_match = self.Stats.call_tid_match + 1
                # self.logger.debug( "Thread [%s] in strace" % str(tid) )
                # return [ x["methId"] for x in self.strace[tid] ]
                # Return the whole thing instead of just the methId field. RLV 3/7/2014
                return [ x for x in self.strace[tid] ]
            else:
                self.Stats.call_tid_nomatch = self.Stats.call_tid_nomatch + 1
                # self.logger.error( "Thread [%s] not in strace" % str(tid) )
        # TODO: How do we check this? RLV 3/6/2014
        # retval = list( self.last_context )
        # return retval
        return [] # TODO: see comment above. What should we do here?

    def log_stats( self ):
        self.logger.debug( "R events: %s" % self.Stats.revents )
        self.logger.debug( "Allocations: %s" % self.Stats.allocates )
        self.logger.debug( "\tAdded twice: %s" % self.Stats.addtwice )
        self.logger.debug( "Updates: %s" % self.Stats.updates )
        self.logger.debug( "\tUpdate nomatch: %s" % self.Stats.update_nomatch )
        self.logger.debug( "Deaths: %s" % self.Stats.deaths )
        self.logger.debug( "\tDeath nomatch: %s" % self.Stats.death_nomatch )
        self.logger.debug( "Thread match: %s" % self.Stats.thread_match )
        self.logger.debug( "\tThread nomatch: %s" % self.Stats.thread_nomatch )
        self.logger.debug( "Method entries: %s" % self.Stats.methentry )
        self.logger.debug( "Method exits: %s" % self.Stats.methexit )
        self.logger.debug( "Method match: %s" % self.Stats.method_match )
        self.logger.debug( "\tMethod nomatch: %s" % self.Stats.method_nomatch )
        self.logger.debug( "No allocation site: %s" % self.Stats.no_allocsite )
        self.logger.debug( "OK receiver object ID: %s" % self.Stats.found_recobjid )
        self.logger.debug( "\tNo receiver object ID: %s" % self.Stats.no_recobjid )
        self.logger.debug( "Call context match: %s" % self.Stats.call_tid_match )
        self.logger.debug( "\tCall context nomatch: %s" % self.Stats.call_tid_nomatch )
        
    def __str__( self ):
        datastr = StringIO.StringIO()
        for tid, stack in self.strace.iteritems():
            if len(stack) > 0:
                topM = stack[-1][1]
                topO = stack[-1][0]
                print >>datastr, "%s [ %d items ] - (m: %s, o: %s)" % \
                    (str(tid), len(stack), str(topM), str(topO))
            else:
                print >>datastr, "%s [ %d items ]" % (str(tid), len(stack))
        return datastr.getvalue()

    class METHOD:
        ENTRY = 1
        EXIT = 2

    def get_most_recent_context( self, threadId ):
        """
        Returns the calling context with most recent first in the list.
        """
        if threadId != None:
            tid = threadId # rename
            if self.has_thread(tid):
                if len(self.strace[tid]) > 0:
                    result = self.strace[tid][-1]
                    return result
                else:
                    self.logger.error( "Empty thread [%s]." % str(tid) )
                    return None
            else:
                self.Stats.call_tid_nomatch = self.Stats.call_tid_nomatch + 1
                self.logger.error( "Thread [%s] not in strace" % str(tid) )
        return None

if __name__ == "__main__":
    import argparse
    import re
    import subprocess
    from etparse import is_valid_op, parse_line, is_heap_alloc_op

    def print_stats( Stats, OtherStats ):
        print "============================================================"
        print "Method entry   : %d" % Stats.methentry
        print "Method exit    : %d" % Stats.methexit
        print "Method match   : %d" % Stats.method_match
        print "Method no match: %d" % Stats.method_nomatch
        print "Thread match   : %d" % Stats.thread_match
        print "Thread no match: %d" % Stats.thread_nomatch
        print "Found rec objID: %d" % Stats.found_recobjid
        print "No rec objID   : %d" % Stats.no_recobjid
        print "Lost method    : %d" % Stats.lost_method
        print "Empty callstack: %d" % Stats.empty_callstack
        print "----------------"
        print "Allocates      : %d" % Stats.allocates
        print "Allocs b4 1st m: %d" % OtherStats.allocs_b4_1st_meth
        print "============================================================"


    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "target", help = "Target trace file in BZ2 format." )
    args = parser.parse_args()

    if not os.path.isfile( args.target ) and not os.path.islink( args.target ):
        # File does not exist
        logger.error( "Unable to open %s" % str(args.target) )
        print "Unable to open %s" % str(args.target)
        exit(21)
    bz2re = re.compile( "(.*)\.bz2$", re.IGNORECASE )
    gzre = re.compile( "(.*)\.gz$", re.IGNORECASE )
    bz2match = bz2re.search( args.target )
    gzmatch = gzre.search( args.target )
    if bz2match: 
        # bzip2 file
        conn = subprocess.Popen( [ "bzcat", args.target ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    elif gzmatch: 
        # gz file
        conn = subprocess.Popen( [ "zcat", args.target ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    else:
        conn = open( args.target, "r")
    Stats = StatClass()
    class OtherStats:
        allocs_b4_1st_meth = 0
    first_method_flag = False
    mytrace = STrace( Stats = Stats )
    version = 5
    for x in conn:
        rec = parse_line( line = x,
                          version = version,
                          logger = mytrace.get_logger() )
        op = rec["rectype"]
        if op == "M":
            # Method entry
            first_method_flag = True
            tid = rec["threadId"]
            methId = rec["methodId"]
            recObjId = rec["objId"]
            if not mytrace.has_thread(tid):
                # print "Adding thread ID [%s]" % str(tid)
                mytrace.enter_method( threadId = tid,
                                      methId = methId,
                                      recObjId = None )
            # STRACE1 mytrace[tid].append( (methId, { "recObjId" : objId }) )
            mytrace.enter_method( threadId = tid,
                                 methId = methId,
                                 recObjId = recObjId )
            # TODO change back to debug
            # print "Enter method %s[%s] -> rec[%s]" % ( str(tid),
            #                                            str(rec["methodId"]),
            #                                            str(recObjId) )
        elif op == "E" or op == "X":
            # Method exit
            # time_by_method = time_by_method + 1
            assert( rec != None )
            tid = rec["threadId"]
            methId = rec["methodId"]
            recObjId = rec["objId"]
            mytrace.exit_method( threadId = tid,
                                 methId = methId,
                                 recObjId = recObjId )
        else:
            if is_heap_alloc_op( op = op, version = version ):
                Stats.allocates = Stats.allocates + 1
                if not first_method_flag:
                    OtherStats.allocs_b4_1st_meth = OtherStats.allocs_b4_1st_meth + 1
                if mytrace.has_thread(rec["threadId"]):
                    Stats.found_recobjid = Stats.found_recobjid + 1
                else:
                    Stats.no_recobjid = Stats.no_recobjid + 1
    print_stats( Stats, OtherStats )
    conn.close()
    print "=================================================================="
    print str(mytrace)

__all__ = [ STrace, StatClass ]
