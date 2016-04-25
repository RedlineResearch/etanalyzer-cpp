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

class ObjectInfoReader:
    def __init__( self,
                  objinfo_file = None,
                  logger = None ):
        self.objinfo_file_name = objinfo_file
        # TODO create logger
        self.objdict = {}
        
    def read_objinfo_file( self ):
        with get_trace_fp(self.objinfo_file_name) as fptr:
            for line in fptr:
                count = 0
                dupeset = set([])
                start = False
                done = False
                debugflag = self.debugflag
                seenset = set([])
                for line in fptr:
                    if line.find("---------------[ OBJECT INFO") == 0:
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
                        objtmp = line.split(",")
                        objtmp[0] = int(objtmp[0])
                        objId = objtmp[0]
                        assert( objId not in self.objdict )
                        self.objdict[objId] = objtmp
                        if objId in seenset:
                            # print "DUP[%s]" % str(x)
                            dupeset.update( [ objId ] )
                        else:
                            seenset.update( [ objId ] )
                        count += 1
        sys.stdout.write("\n")
        sys.stdout.flush()
        print "DUPES:", len(dupeset)
        print "TOTAL:", len(seenset)

class DeathGroupsReader:
    def __init__( self,
                  dgroup_file = None,
                  debugflag = False ):
        self.dgroup_file_name = dgroup_file
        self.dgroups = {}
        self.dgroups_list = []
        self.debugflag = debugflag
        
    def read_dgroup_file( self ):
        with open(self.dgroup_file_name, "rb") as fptr:
            count = 0
            dupeset = set([])
            start = False
            done = False
            debugflag = self.debugflag
            seenset = set([])
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
                    # TODO keyobj = dg[0]
                    # TODO try:
                    # TODO     assert( keyobj not in self.dgroups )
                    # TODO except:
                    # TODO     print "New keyobjId: %s" % str(keyobj)
                    # TODO     pp.pprint(dg)
                    # TODO     exit(100)
                    # TODO self.dgroups[keyobj] = dg[1:]
                    gset = set(dg)
                    self.dgroups_list.append(gset)
                    for x in gset:
                        if x in seenset:
                            # print "DUP[%s]" % str(x)
                            dupeset.update( [ x ] )
                        else:
                            seenset.update( [ x ] )
                    count += 1
                    if debugflag:
                        if count % 1000 == 99:
                            sys.stdout.write("#")
                            sys.stdout.flush()
                            sys.stdout.write(str(len(line)) + " | ")
        sys.stdout.write("\n")
        sys.stdout.flush()
        print "DUPES:", len(dupeset)
        print "TOTAL:", len(seenset)

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

__all__ = [ "GarbologyConfig", "ObjectInfoReader" ]

if __name__ == "__main__":
    main()
