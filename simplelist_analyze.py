from __future__ import division
# simplelist_analyze.py 
#
import argparse
import os
import sys
import time
import logging
import pprint
import re
import ConfigParser
# from operator import itemgetter
from collections import Counter
import csv
from datetime import datetime, date
import time
from collections import defaultdict

from mypytools import mean, stdev, variance
from garbology import SummaryReader, get_index

pp = pprint.PrettyPrinter( indent = 4 )

def setup_logger( targetdir = ".",
                  filename = "simplelist_analyze.log",
                  logger_name = 'simplelist_analyze',
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

def debug_lifetimes( G, cycle, bmark, logger ):
    global pp
    for x in cycle:
        if G.node[x]["lifetime"] <= 0:
            n = G.node[x]
            # print "XXX %s: [ %d - %s ] lifetime: %d" % \
            #     (bmark, x, n["type"], n["lifetime"])
            logger.critical( "XXX: [ %d - %s ] lifetime: %d" %
                             (x, n["type"], n["lifetime"]) )

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

def get_summary( summary_path ):
    start = False
    done = False
    summary = []
    with open(summary_path) as fp:
        for line in fp:
            line = line.rstrip()
            if line.find("---------------[ SUMMARY INFO") == 0:
                start = True if not start else False
                if start:
                    continue
                else:
                    done = True
                    break
            if start:
                row = line.split(",")
                row[1] = int(row[1])
                summary.append(row)
    assert(done)
    return dict(summary)

g_regex = re.compile( "([^\$]+)\$(.*)" )
def is_inner_class( mytype ):
    global g_regex
    m = g_regex.match(mytype)
    return True if m else False

def render_histogram( histfile = None,
                      title = None ):
    outpng = histfile + ".png"
    cmd = [ "/data/rveroy/bin/Rscript",
            "/data/rveroy/pulsrc/etanalyzer/Rgraph/histogram.R", # TODO Hard coded for now.
            # Put into config. TODO TODO TODO
            histfile, outpng,
            "800", "800",
            title, ]
    print "Running histogram.R on %s -> %s" % (histfile, outpng)
    print "[ %s ]" % cmd
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    print "--------------------------------------------------------------------------------"
    for x in result:
        print x
    print "--------------------------------------------------------------------------------"

def write_histogram( results = None,
                     tgtbase  = None,
                     title = None ):
    # TODO Use a list and a for loop to refactor.
    tgtpath_totals = tgtbase + "-totals.csv"
    tgtpath_cycles = tgtbase + "-cycles.csv"
    tgtpath_types = tgtbase + "-types.csv"
    with open(tgtpath_totals, 'wb') as fp_totals, \
         open(tgtpath_cycles, 'wb') as fp_cycles, \
         open(tgtpath_types, 'wb') as fp_types:
        # TODO REFACTOR into a loop
        # TODO 2015-1103 - RLV TODO
        header = [ "benchmark", "total" ]
        csvw = {}
        csvw["totals"] = csv.writer( fp_totals,
                                     quotechar = '"',
                                     quoting = csv.QUOTE_NONNUMERIC )
        csvw["largest_cycle"] = csv.writer( fp_cycles,
                                            quotechar = '"',
                                            quoting = csv.QUOTE_NONNUMERIC )
        csvw["largest_cycle_types_set"] = csv.writer( fp_types,
                                                      quotechar = '"',
                                                      quoting = csv.QUOTE_NONNUMERIC )
        keys = csvw.keys()
        dframe = {}
        for key in keys:
            csvw[key].writerow( header )
            dframe[key] = []
        for benchmark, infodict in results.iteritems():
            for key in keys:
                assert( key in infodict )
                for item in infodict[key]:
                    row = [ benchmark, item ] if key == "totals" \
                          else [ benchmark, len(item) ]
                    dframe[key].append(row)
        sorted_result = [ (key, sorted( dframe[key], key = itemgetter(0) )) for key in keys ]
        for key, result in sorted_result:
            for csvrow in result:
                csvw[key].writerow( csvrow )
    # TODO TODO TODO TODO
    # TODO TODO TODO: SPAWN OFF THREAD
    # TODO TODO TODO TODO
    render_histogram( histfile = tgtpath_totals,
                      title = title )
    render_histogram( histfile = tgtpath_cycles,
                      title = title )
    render_histogram( histfile = tgtpath_types,
                      title = title )

def output_R( benchmark = None ):
    pass
    # Need benchmark.
    # TODO: Do we need this?

def create_work_directory( work_dir,
                           today = "",
                           timenow = "",
                           logger = None,
                           interactive = False ):
    os.chdir( work_dir )
    # Check today directory ---------------------------------------------------
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
    os.chdir( today )
    # Check timenow directory -------------------------------------------------
    if os.path.isfile(timenow):
        print "Can not create %s as directory." % timenow
        exit(11)
    if not os.path.isdir( timenow ):
        os.mkdir( timenow )
    else:
        print "WARNING: %s directory exists." % timenow
        logger.warning( "WARNING: %s directory exists." % timenow )
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    os.chdir( timenow )
    return str(os.getcwd())

def print_summary( summary ):
    global pp
    for bmark, fdict in summary.iteritems():
        print "[%s]:" % bmark
        for key, value in fdict.iteritems():
            if key == "by_size":
                continue
            if key == "types" or key == "sbysize":
                print "    [%s]: %s" % (key, pp.pformat(value))
            else:
                print "    [%s]: %d" % (key, value)

def skip_benchmark(bmark):
    return ( bmark == "tradebeans" or # Permanent ignore
             bmark == "tradesoap" or # Permanent ignore
             bmark != "xalan"
             # bmark == "lusearch" or
             # ( bmark != "batik" and
             #   bmark != "lusearch" and
             #   bmark != "luindex" and
             #   bmark != "specjbb" and
             #   bmark != "avrora" and
             #   bmark != "tomcat" and
             #   bmark != "pmd" and
             #   bmark != "fop"
             # )
           )

def with_primitive_array( typeset = set([]) ):
    typelist = list(typeset)
    arre = re.compile("^\[[CIJ]")
    m1 = arre.search(typelist[1])
    m0 = arre.search(typelist[0])
    if ( (typelist[0].find("[L") == 0) and
         (m1 != None) ):
        return typelist[0]
    elif ( (typelist[1].find("[L") == 0) and
           (m0 != None) ):
        return typelist[1]
    return None


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
        
# Return true if all objects in group are:
#     - primitive
#     - primitive arrays
def all_primitive_types( group = [],
                         objinfo = None ):
    for obj in group:
        mytype = objinfo.get_type(obj)
        if not is_primitive_type(mytype) and not is_primitive_array(mytype):
            return False
    return True


def check_host( benchmark = None,
                worklist_config = {},
                host_config = {} ):
    thishost = socket.gethostname()
    for wanthost in worklist_config[benchmark]:
        if thishost in host_config[wanthost]:
            return True
    return False

def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None

def main_process( output = None,
                  global_config = {},
                  summary_config = {},
                  main_config = {},
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO 2016 August 7 TODO
    # This is where the summary CSV files are
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    # Setup stdout to file redirect TODO: Where should this comment be placed?
    # TODO: Eventually remove the following commented code related to hosts.
    # Since we're not doing mutiprocessing, we don't need this. But keep
    # it here until absolutely sure.
    # TODO: thishost = get_actual_hostname( hostname = socket.gethostname().lower(),
    # TODO:                                 host_config = host_config )
    # TODO: assert( thishost != None )
    # TODO: thishost = thishost.upper()
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
    # Take benchmarks to process from etanalyze_config
    # The benchmarks are:
    #     BENCHMARK   |   CREATE  |  DELETE   |
    #     simplelist1 |    seq    |    seq    |
    #     simplelist2 |   rand    |    seq    |
    #     simplelist3 |    seq    |    at end |
    #     simplelist4 |   rand    |    at end |
    # Where to get file?
    # Filename is in "summary_config"
    # Directory is in "global_config"
    #     Make sure everything is honky-dory.
    assert( "cycle_cpp_dir" in global_config )
    assert( "simplelist1" in summary_config )
    assert( "simplelist2" in summary_config )
    assert( "simplelist3" in summary_config )
    assert( "simplelist4" in summary_config )
    # Give simplelist? more descriptive names
    slist = { "SEQ-SEQ" : {}, # simplelist1
              "RAND-SEQ" : {}, # simplelist2
              "SEQ-ATEND" : {}, # simplelist3
              "RAND-ATEND" : {}, } # simplelist4
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    print "XXX:", os.path.join( cycle_cpp_dir, summary_config["simplelist1"] )
    slist["SEQ-SEQ"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                               summary_config["simplelist1"] ) )
    slist["RAND-SEQ"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                summary_config["simplelist2"] ) )
    slist["SEQ-ATEND"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                 summary_config["simplelist3"] ) )
    slist["RAND-ATEND"]["sreader"] = SummaryReader( os.path.join( cycle_cpp_dir,
                                                                  summary_config["simplelist4"] ) )

    print "====[ Reading in the summaries ]================================================"
    for skind, mydict in slist.iteritems():
        sreader = mydict["sreader"]
        sreader.read_summary_file()
        pp.pprint( sreader.__get_summarydict__() )
    print "DONE reading all 4."
    print "================================================================================"
    print "simplelist_analyze.py - DONE."
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
    summary_config = config_section_map( "summary_cpp", config_parser )
    main_config = config_section_map( "simplelist-analyze", config_parser )
    # MAYBE: objectinfo_config = config_section_map( "objectinfo", config_parser )
    # DON'T KNOW: contextcount_config = config_section_map( "contextcount", config_parser )
    # PROBABLY NOT:  host_config = config_section_map( "hosts", config_parser )
    # PROBABLY NOT: worklist_config = config_section_map( "dgroups-worklist", config_parser )
    return { "global" : global_config,
             "summary" : summary_config,
             "main" : main_config,
             # "objectinfo" : objectinfo_config,
             # "contextcount" : contextcount_config,
             # "host" : host_config,
             # "worklist" : worklist_config
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
    parser.add_argument( "output", help = "Target output filename." )
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
    parser.add_argument( "--logfile",
                         help = "Specify logfile name.",
                         action = "store" )
    parser.set_defaults( logfile = "simplelist_analyze.log",
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
    summary_config = configdict["summary"]
    main_config = configdict["main"]
    # PROBABLY DELETE:
    # contextcount_config = configdict["contextcount"]
    # objectinfo_config = configdict["objectinfo"]
    # host_config = process_host_config( configdict["host"] )
    # worklist_config = process_worklist_config( configdict["worklist"] )
    # Set up logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         global_config = global_config,
                         summary_config = summary_config,
                         main_config = main_config,
                         # contextcount_config = contextcount_config,
                         # objectinfo_config = objectinfo_config,
                         # host_config = host_config,
                         # worklist_config = worklist_config,
                         logger = logger )

if __name__ == "__main__":
    main()
