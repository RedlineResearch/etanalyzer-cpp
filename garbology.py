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

def index( field = None ):
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
    return ( rec[index("GARBTYPE")] == "CYCKEY" or
             rec[index("GARBTYPE")] == "DAGKEY" )


class ObjectInfoReader:
    def __init__( self,
                  objinfo_file = None,
                  logger = None ):
        self.objinfo_file_name = objinfo_file
        # TODO create logger
        self.objdict = {}
        self.typedict = {}
        self.rev_typedict = {}
        self.keyset = set([])
        self.logger = logger

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
                        if is_key_object( object_info[objId] ):
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

class DeathGroupsReader:
    def __init__( self,
                  dgroup_file = None,
                  debugflag = False,
                  logger = None ):
        self.dgroup_file_name = dgroup_file
        self.dgroups = {}
        self.debugflag = debugflag
        self.logger = logger
        
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
            withkey = 0
            withoutkey = 0
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
                    # TODO This shouldn't be necessary anymore with new change
                    #      but will still work even if there's no more terminating
                    #      comma.
                    dg = [ int(x) for x in line.split(",") ]
                    keylist = get_key_objects( dg, object_info_reader )
                    gset = set(dg)
                    if len(keylist) > 1:
                        print "X:", str(dg)
                        withkey += 1
                    elif len(keylist) == 0:
                        print "Z:", str(dg)
                        withoutkey += 1
                    # TODO for x in gset:
                    # TODO     if x in seenset:
                    # TODO         # print "DUP[%s]" % str(x)
                    # TODO         dupeset.update( [ x ] )
                    # TODO     else:
                    # TODO         seenset.update( [ x ] )
                    # TODO count += 1
                    if debugflag:
                        if count % 1000 == 99:
                            sys.stdout.write("#")
                            sys.stdout.flush()
                            sys.stdout.write(str(len(line)) + " | ")
        #sys.stdout.write("\n")
        #sys.stdout.flush()
        #print "DUPES:", len(dupeset)
        #print "TOTAL:", len(seenset)
        print "With key: %d" % withkey
        print "Without key: %d" % withoutkey

    def iteritems( self ):
        return self.dgroups.iteritems()

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

__all__ = [ "GarbologyConfig", "ObjectInfoReader", "is_key_object", "index", ]

if __name__ == "__main__":
    main()
