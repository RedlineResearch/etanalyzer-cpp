# garbology.py 
#
import argparse
import os
import sys
import logging
import pprint
import re
import ConfigParser
import csv
import datetime
import subprocess
from collections import defaultdict

pp = pprint.PrettyPrinter( indent = 4 )

#
#  PUBLIC
#
class GarbologyConfig:
    def __init__( self, config_file ):
        self.config_file_name = config_file
        self.config_parser = ConfigParser.ConfigParser()
        self.config_parser.read( config_file )
        self.process_config( config_file )

    def config_section_map( self, section, config_parser ):
        result = {}
        options = config_parser.options(section)
        for option in options:
            try:
                result[option] = config_parser.get(section, option)
            except:
                print("exception on %s!" % option)
                result[option] = None
        return result

    def process_config( self, config_file ):
        cp = self.config_parser
        self.configdict = {}
        cdict = self.configdict
        self.global_cfg = self.config_section_map( "global", cp )
        cdict["global"] = self.global_cfg
        self.etanalyze_cfg = self.config_section_map( "etanalyze-output", cp )
        cdict["etanalyze"] = self.etanalyze_cfg
        self.cycle_analyze_cfg = self.config_section_map( "cycle-analyze", cp )
        cdict["cycle_analyze"] = self.cycle_analyze_cfg
        self.edgeinfo_cfg = self.config_section_map( "edgeinfo", cp )
        cdict["edgeinfo"] = self.edgeinfo_cfg
        self.objectinfo_cfg = self.config_section_map( "objectinfo", cp )
        cdict["objectinfo"] = self.objectinfo_cfg
        self.summary_cfg = self.config_section_map( "summary_cpp", cp )
        cdict["summary"] = self.summary_cfg
        self.dsites_cfg = self.config_section_map( "dsites", cp )
        cdict["dsites"] = self.dsites_cfg

    def verify_all_exist( self, printflag = False ):
        cdict = self.configdict
        basepath = cdict["global"]["cycle_cpp_dir"]
        print "PATH:", basepath
        for key, cfg in self.configdict.iteritems():
            if key == "global" or key == "cycle_analyze":
                continue
            if printflag:
                print "[%s]" % key
            for bmark, relpath in cfg.iteritems():
                tgtpath = basepath + relpath
                if not os.path.isfile(tgtpath):
                    print "ERROR: %s" % str(tgtpath)
                elif printflag:
                    print "%s - OK." % str(tgtpath)

    def print_all_config( self, mypp ):
        print "-------------------------------------------------------------------------------"
        for key, cfg in self.configdict.iteritems():
            print "[%s]" % str(key)
            mypp.pprint( cfg )
            print "-------------------------------------------------------------------------------"

def get_index( field = None ):
    try:
        return { "ATIME" : 0,
                 "DTIME" : 1,
                 "SIZE"  : 2,
                 "TYPE"  : 3,
                 "DIEDBY" : 4,
                 "LASTUP" : 5,
                 "STATTR" : 6,
                 "GARBTYPE" : 7,
        }[field]
    except:
        return None

def is_key_object( rec = None ):
    return ( rec[get_index("GARBTYPE")] == "CYCKEY" or
             rec[get_index("GARBTYPE")] == "DAGKEY" )

def get_key_objects( idlist = [],
                     object_info_reader = None ):
    oir = object_info_reader
    result = []
    for objId in idlist:
        rec = oir.get_record( objId )
        assert( rec != None )
        if is_key_object(rec):
            result.append(rec)
    return result


# ----------------------------------------------------------------------------- 
# ----------------------------------------------------------------------------- 
class ObjectInfoReader:
    def __init__( self,
                  objinfo_file = None,
                  logger = None ):
        # TODO: Choice of loading from text file or from pickle
        self.objinfo_file_name = objinfo_file
        self.objdict = {}
        self.typedict = {}
        self.rev_typedict = {}
        self.keyset = set([])
        self.logger = logger

    def is_key_object( self, objId = None ):
        assert(type(objId) == type(1))
        if objId in self.objdict:
            od = self.objdict
            return ( od[objId][get_index("GARBTYPE")] == "CYCKEY" or
                     od[objId][get_index("GARBTYPE")] == "DAGKEY" )
        else:
            return False

    def read_objinfo_file( self ):
        start = False
        done = False
        object_info = self.objdict
        with get_trace_fp(self.objinfo_file_name) as fp:
            for line in fp:
                line = line.rstrip()
                if line.find("---------------[ OBJECT INFO") == 0:
                    start = True if not start else False
                    if start:
                        continue
                    else:
                        done = True
                        break
                if start:
                    rowtmp = line.split(",")
                    # 0 - allocation time
                    # 1 - death time
                    # 2 - size
                    row = [ int(x) for x in rowtmp[1:4] ]
                    mytype = rowtmp[-2]
                    row.append( self.get_typeId( mytype ) )
                    row.extend( rowtmp[5:] )
                    objId = int(rowtmp[0])
                    if objId not in object_info:
                        object_info[objId] = tuple(row)
                        if self.is_key_object( objId ):
                            self.keyset.add( objId )
                    else:
                        self.logger.error( "DUPE: %s" % str(objId) )
        assert(done)

    def get_typeId( self, mytype ):
        typedict = self.typedict
        rev_typedict = self.rev_typedict
        if mytype in typedict:
            return typedict[mytype]
        else:
            lastkey = len(typedict.keys())
            typedict[mytype] = lastkey + 1
            rev_typedict[lastkey + 1] = mytype
            return lastkey + 1

    def died_at_end( self, objId ):
        return (self.objdict[objId][get_index("DIEDBY")] == "E") if (objId in self.objdict) \
            else False

    def iteritems( self ):
        return self.objdict.iteritems()

    # If numlines == 0, print out all.
    def print_out( self, numlines = 30 ):
        count = 0
        for objId, rec in self.objdict.iteritems():
            print "%d -> %s" % (objId, str(rec))
            count += 1
            if numlines != 0 and count >= numlines:
                break

    def get_record( self, objId = 0 ):
        return self.objdict[objId] if (objId in self.objdict) else None

    def get_type( self, objId = 0 ):
        return self.objdict[objId][get_index("TYPE")] if (objId in self.objdict) else None

    def died_by_stack( self, objId = 0 ):
        return (self.objdict[objId][get_index("DIEDBY")] == "S") if (objId in self.objdict) \
            else False

    def verify_died_by( self,
                        grouplist = [],
                        died_by = None,
                        fail_on_missing = False ):
        assert( died_by == "S" or died_by == "H" or died_by == "E" )
        flag = True
        for obj in grouplist:
            if obj not in self.objdict:
                self.logger.critical( "Missing object: %d" % obj )
                if fail_on_missing:
                    return False
                continue
            else:
                rec = self.objdict[obj]
                if rec[ get_index("DIEDBY") ] != died_by:
                    self.logger.error( "Looking for '%s' - found '%s'" %
                                       (died_by, rec[ get_index("DIEDBY") ]) )
                    flag = False
        return flag


# ----------------------------------------------------------------------------- 
# ----------------------------------------------------------------------------- 
class EdgeInfoReader:
    def __init__( self,
                  edgeinfo_file = None,
                  logger = None ):
        # TODO: Choice of loading from text file or from pickle
        # 
        self.edgeinfo_file_name = edgeinfo_file
        # Edge dictionary
        self.edgedict = {} # (src, tgt) -> (create time, death time)
        # Source to target object dictionary
        self.srcdict = defaultdict( set ) # src -> set of tgts
        # Target to incoming source object dictionary
        self.tgtdict = defaultdict( set ) # tgt -> set of srcs
        # Target object to record of last edge
        self.lastedge = {} # tgt -> (list of lastedges, death time)
        self.logger = logger

    def read_edgeinfo_file( self ):
        start = False
        done = False
        edge_info = self.edgedict
        with get_trace_fp(self.edgeinfo_file_name) as fp:
            for line in fp:
                line = line.rstrip()
                if line.find("---------------[ EDGE INFO") == 0:
                    start = True if not start else False
                    if start:
                        continue
                    else:
                        done = True
                        break
                if start:
                    rowtmp = line.split(",")
                    # 0 - srcId
                    # 1 - tgtId
                    # 2 - create time 
                    # 3 - death time 
                    row = [ int(x) for x in rowtmp ]
                    src = row[0]
                    tgt = row[1]
                    timepair = tuple(row[2:])
                    dtime = row[3]
                    self.edgedict[tuple([src, tgt])] = timepair
                    self.srcdict[src].add( tgt )
                    self.tgtdict[tgt].add( src )
                    self.update_last_edges( src = src,
                                            tgt = tgt,
                                            deathtime = dtime )
        assert(done)

    def get_targets( self, src = 0 ):
        if src in self.srcdict:
            return self.srcdict[src]
        else:
            return []

    def get_sources( self, tgt = 0 ):
        if tgt in self.tgtdict:
            return self.tgtdict[tgt]
        else:
            return []

    def edgedict_iteritems( self ):
        return self.edgedict.iteritems()

    def srcdict_iteritems( self ):
        return self.srcdict.iteritems()

    def tgtdict_iteritems( self ):
        return self.tgtdict.iteritems()

    def lastedge_iteritems( self ):
        return self.lastedge.iteritems()

    def print_out( self, numlines = 30 ):
        count = 0
        for edge, timepaid in self.edgedict.iteritems():
            print "(%d, %d) -> (%d, %d)" % (edge[0], edge[1], timepaid[0], timepaid[1])
            count += 1
            if numlines != 0 and count >= numlines:
                break

    def get_edge_times( self, edge = None ):
        if edge in self.edgedict:
            return self.edgedict[ edge ]
        else:
            return (None, None)

    def update_last_edges( self,
                           src = None,
                           tgt = None,
                           deathtime = None ):
        # Given a target, find what the sources are
        if tgt in self.lastedge:
            if self.lastedge[tgt]["dtime"] < deathtime:
                self.lastedge[tgt] = { "lastsources" : [ src ],
                                       "dtime" : deathtime }
            elif self.lastedge[tgt]["dtime"] == deathtime:
                self.lastedge[tgt]["lastsources"].append(src)
        else:
            self.lastedge[tgt] = { "lastsources" : [ src ],
                                   "dtime" : deathtime }
    
    def get_last_edge_record( self, tgtId = None ):
        return self.lastedge[tgtId] if tgtId in self.lastedge else None


# ----------------------------------------------------------------------------- 
# ----------------------------------------------------------------------------- 
class DeathGroupsReader:
    def __init__( self,
                  dgroup_file = None,
                  debugflag = False,
                  logger = None ):
        self.dgroup_file_name = dgroup_file
        # Map of object to list of group numbers
        self.obj2group = {}
        # Map of key to group number
        self.key2group = {}
        # Map of key to death time 
        self.group2dtime = {}
        # Map of group number to list of objects
        self.group2list= {}
        self.debugflag = debugflag
        self.logger = logger
        
    def map_key2group( self,
                       groupnum = 0,
                       keylist = [] ):
        assert( groupnum > 0 )
        k2g = self.key2group
        for k in keylist:
            if k in k2g:
                k2g[k].append( groupnum )
            else:
                k2g[k] = [ groupnum ]

    def map_obj2group( self,
                       groupnum = 0,
                       groupset = set([]) ):
        assert( groupnum > 0 )
        ogroup = self.obj2group
        for obj in groupset:
            if obj in ogroup:
                ogroup[obj].append( groupnum )
            else:
                ogroup[obj] = [ groupnum ]

    def map_group2dtime( self,
                         groupnum = 0,
                         dtime = 0 ):
        assert( groupnum > 0 )
        self.group2dtime[groupnum] = dtime
        # NOTE: This is made into a function because there may be
        # other things we wish to do with saving the sets of death
        # times.

    def move_group( self,
                    src = None,
                    tgt = None ):
        if src in self.group2list:
            if tgt in self.group2list:
                self.group2list[src].extend( self.group2list[tgt] )
                del self.group2list[tgt]
            else:
                self.logger.critical( "%d not found." % tgt )
        else:
            self.logger.critical( "%d not found." % src )

    def read_dgroup_file( self,
                          object_info_reader = None ):
        # We don't know which are the key objects. TODO TODO TODO
        with open(self.dgroup_file_name, "rb") as fptr:
            count = 0
            dupeset = set([])
            start = False
            done = False
            debugflag = self.debugflag
            seenset = set([])
            multkey = 0
            # withkey = 0
            withoutkey = 0
            groupnum = 1
            logger = self.logger
            oir = object_info_reader
            dtind = get_index("DTIME")
            for line in fptr:
                if line.find("---------------[ CYCLES") == 0:
                    start = True if not start else False
                    if start:
                        continue
                    else:
                        done = True
                        break
                if start:
                    line = line.rstrip()
                    line = line.rstrip(",")
                    # Remove all objects that died at program end.
                    dg = [ int(x) for x in line.split(",") if not oir.died_at_end(int(x))  ]
                    if len(dg) == 0:
                        continue
                    dtimes = list( set( [ oir.get_record(x)[dtind] for x in dg ] ) )
                    if (len(dtimes) > 1):
                        # split into groups according to death times
                        logger.debug( "Multiple death times: %s" % str(dtimes) )
                    dglist = []
                    for ind in xrange(len(dtimes)):
                        dtime = dtimes[ind]
                        mydg = [ x for x in dg if oir.get_record(x)[dtind] == dtime ]
                        dglist.append( mydg )
                    assert(len(dglist) == len(dtimes))
                    for ind in xrange(len(dglist)):
                        dg = list( set( dglist[ind] ) )
                        dtime = dtimes[ind]
                        self.map_obj2group( groupnum = groupnum, groupset = dg )
                        self.map_group2dtime( groupnum = groupnum, dtime = dtime )
                        self.group2list[groupnum] = dg
                        groupnum += 1
                    if debugflag:
                        if count % 1000 == 99:
                            sys.stdout.write("#")
                            sys.stdout.flush()
                            sys.stdout.write(str(len(line)) + " | ")
        #sys.stdout.write("\n")
        #sys.stdout.flush()
        #print "DUPES:", len(dupeset)
        #print "TOTAL:", len(seenset)
        moved = {}
        loopflag = True
        while loopflag:
            loopflag = False
            for obj, groups in self.obj2group.iteritems():
                if len(groups) > 1:
                    # Merge into lower group number.
                    gsort = sorted( [ x for x in groups if (x not in moved and x in self.group2list) ] )
                    if len(gsort) < 2:
                        continue
                    tgt = gsort[0]
                    for gtmp in gsort[1:]:
                        # Add to target group
                        if gtmp in self.group2list:
                            loopflag = True
                            self.group2list[tgt].extend( self.group2list[gtmp] )
                            moved[gtmp] = tgt
                            # Remove the merged group
                            del self.group2list[gtmp]
                            # TODO TODO TODO
                            # Fix the obj2group when we delete from group2list
                        # TODO Should we remove from other dictionaries?
        print "----------------------------------------------------------------------"
        # TODO grlen = sorted( [ len(mylist) for group, mylist in self.group2list.iteritems() if len(mylist) > 0 ],
        #                       reverse = True )
        for gnum, mylist in self.group2list.iteritems():
            keylist = get_key_objects( mylist, oir )
            self.map_key2group( groupnum = groupnum, keylist = keylist )
            # Debug key objects. NOTE: This may not be used for now.
            if len(keylist) > 1:
                logger.error( "multiple key objects: %s" % str(keylist) )
                multkey += 1
            elif len(keylist) == 0:
                logger.critical( "NO key object in group: %s" % str(dg) )
                withoutkey += 1
        print "Multiple key: %d" % multkey
        print "Without key: %d" % withoutkey
        print "----------------------------------------------------------------------"

    def iteritems( self ):
        return self.group2list.iteritems()

    def clean_deathgroups( self ):
        group2list = self.group2list
        count = 0
        for gnum in group2list.keys():
            if len(group2list[gnum]) == 0:
                del group2list[gnum]
                count += 0
            else:
                group2list[gnum] = list(set(group2list[gnum]))
        print "%d empty groups cleaned." % count

# ----------------------------------------------------------------------------- 
# ----------------------------------------------------------------------------- 
#
#  PRIVATE FUNCTIONS
#

def setup_logger( targetdir = ".",
                  filename = "garbology.log",
                  logger_name = 'garbology',
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

def create_work_directory( work_dir, logger = None, interactive = False ):
    os.chdir( work_dir )
    today = datetime.date.today()
    today = today.strftime("%Y-%m%d")
    if os.path.isfile(today):
        print "Can not create %s as directory." % today
        exit(11)
    if not os.path.isdir( today ):
        os.mkdir( today )
    else:
        print "WARNING: %s directory exists." % today
        logger.warning( "WARNING: %s directory exists." % today )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    return today

def main_process( logger = None,
                  gconfig = None,
                  debugflag = False,
                  verbose = False ):
    global pp
    gconfig.print_all_config( pp )
    gconfig.verify_all_exist( printflag = verbose )
    print "===========[ DONE ]==================================================="

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "config",
                         help = "Specify configuration filename." )
    parser.add_argument( "--verbose",
                         dest = "verbose",
                         help = "Enable verbose output.",
                         action = "store_true" )
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "garbology.log",
                         verbose = False,
                         config = None )
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    assert( args.config != None )
    assert( os.path.isfile( args.config ) )
    gconfig = GarbologyConfig( args.config )
    debugflag = gconfig.global_cfg["debug"]
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = debugflag )
    #
    # Main processing
    #
    return main_process( logger = logger,
                         gconfig = gconfig,
                         debugflag = debugflag,
                         verbose = args.verbose )

__all__ = [ "EdgeInfoReader", "GarbologyConfig", "ObjectInfoReader", "is_key_object", "get_index", ]

if __name__ == "__main__":
    main()
