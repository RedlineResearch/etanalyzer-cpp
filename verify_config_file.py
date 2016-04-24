# update_main_config.py 
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

def setup_logger( targetdir = ".",
                  filename = "update_main_config.log",
                  logger_name = 'update_main_config',
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

def main_process( output = None,
                  main_config = None,
                  benchmark = None,
                  lastedgeflag = False,
                  etanalyze_config = None,
                  global_config = None,
                  edge_config = None,
                  edgeinfo_config = None,
                  objectinfo_config = None,
                  summary_config = None,
                  debugflag = False,
                  logger = None ):
    global pp
    # HERE: TODO
    # 1. Cyclic garbage vs ref count reclaimed:
    #      * Number of objects
    #      * size of objects
    # 2. Number of cycles
    # 3. Size of cycles
    print "GLOBAL:"
    pp.pprint(global_config)
    cycle_cpp_dir = global_config["cycle_cpp_dir"]
    work_dir = main_config["directory"]
    results = {}
    summary = {}
    typedict = {} # Type dictionary is ACROSS all benchmarks
    rev_typedict = {} # Type dictionary is ACROSS all benchmarks
    count = 0
    today = create_work_directory( work_dir, logger = logger )
    olddir = os.getcwd()
    os.chdir( today )
    for bmark, filename in etanalyze_config.iteritems():
        # if skip_benchmark(bmark):
        if ( (benchmark != "_ALL_") and (bmark != benchmark) ):
            print "SKIP:", bmark
            continue
        print "=======[ %s ]=========================================================" \
            % bmark
        logger.critical( "=======[ %s ]=========================================================" 
                         % bmark )
        abspath = os.path.join(cycle_cpp_dir, filename)
        if not os.path.isfile(abspath):
            logger.critical("Not such file: %s" % str(abspath))
        else:
            group = 1
            graphs = []
            # Counters TODO: do we need this?
            cycle_total_counter = Counter()
            actual_cycle_counter = Counter()
            cycle_type_counter = Counter()
            logger.critical( "Opening %s." % abspath )
            # Get cycles
            cycles = get_cycles( abspath )
            # TODO What is this? 
            # TODO get_cycles_result = {}
            # Get edges
            edgepath = os.path.join(cycle_cpp_dir, edge_config[bmark])
            edges = get_edges( edgepath )
            # Get edge information
            edgeinfo_path = os.path.join(cycle_cpp_dir, edgeinfo_config[bmark])
            edge_info_dict = get_edge_info( edgeinfo_path)
            # Get object dictionary information that has types and sizes
            objectinfo_path = os.path.join(cycle_cpp_dir, objectinfo_config[bmark])
            object_info_dict = get_object_info( objectinfo_path, typedict, rev_typedict )
            # Get summary
            summary_path = os.path.join(cycle_cpp_dir, summary_config[bmark])
            summary_sim = get_summary( summary_path )
            #     get summary by size
            number_of_objects = summary_sim["number_of_objects"]
            number_of_edges = summary_sim["number_of_edges"]
            died_by_stack = summary_sim["died_by_stack"]
            died_by_heap = summary_sim["died_by_heap"]
            died_by_stack_after_heap = summary_sim["died_by_stack_after_heap"]
            died_by_stack_only = summary_sim["died_by_stack_only"]
            died_by_stack_after_heap_size = summary_sim["died_by_stack_after_heap_size"]
            died_by_stack_only_size = summary_sim["died_by_stack_only_size"]
            size_died_by_stack = summary_sim["size_died_by_stack"]
            size_died_by_heap = summary_sim["size_died_by_heap"]
            last_update_null = summary_sim["last_update_null"]
            last_update_null_heap = summary_sim["last_update_null_heap"]
            last_update_null_stack = summary_sim["last_update_null_stack"]
            last_update_null_size = summary_sim["last_update_null_size"]
            last_update_null_heap_size = summary_sim["last_update_null_heap_size"]
            last_update_null_stack_size = summary_sim["last_update_null_stack_size"]
            max_live_size = summary_sim["max_live_size"]
            final_time = summary_sim["final_time"]
            selfloops = set()
            edgedict = create_edge_dictionary( edges, selfloops )
            results[bmark] = { "totals" : [],
                               "graph" : [],
                               "largest_cycle" : [],
                               "largest_cycle_types_set" : [],
                               "lifetimes" : [],
                               "lifetime_mean" : [],
                               "lifetime_sd" : [],
                               "lifetime_max" : [],
                               "lifetime_min" : [],
                               "sizes_largest_scc" : [],
                               "sizes_all" : [], }
            summary[bmark] = { "by_size" : { 1 : [], 2 : [], 3 : [], 4 : [] },
                               # by_size contains apriori sizes 1 to 4 and the
                               # cycles with these sizes. The cycle is encoded
                               # as a list of object IDs (objId). by_size here means by cycle size
                               "died_by_heap" : died_by_heap, # total of
                               "died_by_stack" : died_by_stack, # total of
                               "died_by_stack_after_heap" : died_by_stack_after_heap, # subset of died_by_stack
                               "died_by_stack_only" : died_by_stack_only, # subset of died_by_stack
                               "died_by_stack_after_heap_size" : died_by_stack_after_heap_size, # size of
                               "died_by_stack_only_size" : died_by_stack_only_size, # size of
                               "last_update_null" : last_update_null, # subset of died_by_heap
                               "last_update_null_heap" : last_update_null_heap, # subset of died_by_heap
                               "last_update_null_stack" : last_update_null_stack, # subset of died_by_heap
                               "last_update_null_size" : last_update_null_size, # size of
                               "last_update_null_heap_size" : last_update_null_heap_size, # size of
                               "last_update_null_stack_size" : last_update_null_stack_size, # size of
                               "max_live_size" : max_live_size,
                               "number_of_objects" : number_of_objects,
                               "number_of_edges" : number_of_edges,
                               "number_of_selfloops" : 0,
                               "types" : Counter(), # counts of types using type IDs
                               "size_died_by_stack" : size_died_by_stack, # size, not object count
                               "size_died_by_heap" : size_died_by_heap, # size, not object count
                               }
            for index in xrange(len(cycles)):
                cycle = cycles[index]
                cycle_info_list = get_cycle_info_list( cycle = cycle,
                                                       objinfo_dict = object_info_dict,
                                                       # objdb,
                                                       logger = logger )
                if len(cycle_info_list) == 0:
                    continue
                # GRAPH
                G = create_graph( cycle_info_list = cycle_info_list,
                                  edgedict = edgedict,
                                  logger = logger )
                # Get the actual cycle - LARGEST
                # Sanity check 1: Is it a DAG?
                if nx.is_directed_acyclic_graph(G):
                    logger.error( "Not a cycle." )
                    logger.error( "Nodes: %s" % str(G.nodes()) )
                    logger.error( "Edges: %s" % str(G.edges()) )
                    continue
                ctmplist = list( nx.simple_cycles(G) )
                # Sanity check 2: Check to see it's not empty.
                if len(ctmplist) == 0:
                    # No cycles!!!
                    logger.error( "Not a cycle." )
                    logger.error( "Nodes: %s" % str(G.nodes()) )
                    logger.error( "Edges: %s" % str(G.edges()) )
                    continue
                # TODO TODO TODO
                # Interesting cases are:
                # - largest is size 1 (self-loops)
                # - multiple largest cycles?
                #     * Option 1: choose only one?
                #     * Option 2: ????
                # 
                # Get Strongly Connected Components
                scclist = list(nx.strongly_connected_components(G))
                # Strong connected-ness is a better indication of what we want
                # Unless the cycle is a single node with a self pointer.
                # TOTALS - size of the whole component including leaves
                results[bmark]["totals"].append( len(cycle) )
                cycle_total_counter.update( [ len(cycle) ] )
                # Append graph too
                results[bmark]["graph"].append(G)
                largest_scc = append_largest_SCC( ldict = results[bmark]["largest_cycle"],
                                                  scclist = scclist,
                                                  selfloops = selfloops,
                                                  logger = logger )
                if len(largest_scc) == 1:
                    summary[bmark]["number_of_selfloops"] += 1
                # Cycle length counter
                actual_cycle_counter.update( [ len(largest_scc) ] )
                # Get the types and type statistics
                largest_by_types_with_index = get_types_and_save_index( G, largest_scc )
                largest_by_types = [ x[1] for x in largest_by_types_with_index ]
                summary[bmark]["types"].update( largest_by_types )
                largest_by_types_set = set(largest_by_types)
                # Save small cycles 
                save_interesting_small_cycles( largest_by_types_with_index, summary[bmark] )
                # TYPE SET
                results[bmark]["largest_cycle_types_set"].append(largest_by_types_set)
                cycle_type_counter.update( [ len(largest_by_types_set) ] )
                group += 1
                # LIFETIME
                lifetimes = get_lifetimes( G, largest_scc )
                if lastedgeflag:
                    # GET LAST EDGE
                    last_edge = get_last_edge( largest_scc, edge_info_db )
                else:
                    last_edge = None
                debug_lifetimes( G = G,
                                 cycle = cycle,
                                 bmark = bmark, 
                                 logger = logger )
                if len(lifetimes) >= 2:
                    ltimes_mean = mean( lifetimes )
                    ltimes_sd = stdev( lifetimes, ltimes_mean )
                elif len(lifetimes) == 1:
                    ltimes_mean = lifetimes[0]
                    ltimes_sd = 0
                else:
                    raise ValueError("No lifetime == no node found?")
                results[bmark]["lifetimes"].append(lifetimes)
                results[bmark]["lifetime_mean"].append(ltimes_mean)
                results[bmark]["lifetime_sd"].append(ltimes_sd)
                results[bmark]["lifetime_max"].append( max(lifetimes) )
                results[bmark]["lifetime_min"].append( min(lifetimes) )
                # End LIFETIME
                # SIZE PER TYPE COUNT
                # Per bencmark:
                #   count of types -> size in bytes
                #   then group accoring to count of types:
                #         count -> [ size1, s2, s3, ... sn ]
                #   * graph (option 1)
                #   * stats (option 2)
                #   * option3? ? ?
                cycle_sizes = get_sizes( G, largest_scc )
                total_sizes = get_sizes( G, cycle )
                results[bmark]["sizes_largest_scc"].append(cycle_sizes)
                results[bmark]["sizes_all"].append(total_sizes)
                # End SIZE PER TYPE COUNT
            largelist = save_largest_cycles( results[bmark]["graph"], num = 5 )
            # Make directory and Cd into directory
            if not os.path.isdir(bmark):
                os.mkdir(bmark)
            for_olddir = os.getcwd()
            os.chdir( bmark )
            # Create the CSV files for the data
            small_result = extract_small_cycles( small_summary = summary[bmark]["by_size"], 
                                                 bmark = bmark,
                                                 objinfo_dict = object_info_dict,
                                                 rev_typedict = rev_typedict,
                                                 logger = logger ) 
            print "================================================================================"
            total_small_cycles = small_result["total_cycles"]
            inner_classes_count = small_result["inner_classes_count"]
            # Cd back into parent directory
            os.chdir( for_olddir )
            print "--------------------------------------------------------------------------------"
            print "num_cycles: %d" % len(cycles)
            print "number of types:", len(summary[bmark]["types"])
            print "cycle_total_counter:", str(cycle_total_counter)
            print "actual_cycle_counter:", str(actual_cycle_counter)
            print "cycle_type_counter:", str(cycle_type_counter)
            print "total small cycles:", total_small_cycles
            print "inner_classes_count:", str(inner_classes_count)
            print "--------------------------------------------------------------------------------"
        count += 1
        # if count >= 1:
        #     break
    # benchmark:
    # size, 1, 4, 5, 2, etc
    # largest_cycle, 1, 2, 5, 1, etc
    # number_types, 1, 1, 2, 1, etc
    # TODO - fix this documentation
    print "======================================================================"
    print "===========[ RESULTS ]================================================"
    output_results_transpose( output_path = output,
                              results = results )
    output_summary( output_path = output,
                    summary = summary )
    os.chdir( olddir )
    # Print out results in this format:
    print "===========[ SUMMARY ]================================================"
    print_summary( summary )
    # TODO: Save the largest X cycles.
    #       This should be done in the loop so to cut down on duplicate work.
    print "===========[ TYPES ]=================================================="
    benchmarks = summary.keys()
    pp.pprint(benchmarks)
    # TODO
    print "---------------[ Common to ALL ]--------------------------------------"
    # common_all = set.intersection( *[ set(summary[b]["types"].keys()) for b in benchmarks ] )
    # common_all = [ rev_typedict[x] for x in common_all ]
    # pp.pprint( common_all )
    # print "---------------[ Counter over all benchmarks ]------------------------"
    # g_types = Counter()
    # for bmark, bdict in summary.iteritems():
    #     g_types.update( bdict["types"] )
    # for key, value in g_types.iteritems():
    #     print "%s: %d" % (rev_typedict[key], value)
    # print "Number of types - global: %d" % len(g_types)
    print "===========[ DONE ]==================================================="
    exit(1000)

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "config",
                         help = "Specify configuration filename." )
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
    parser.set_defaults( logfile = "update_main_config.log",
                         debugflag = False,
                         config = None )
    return parser

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
    etanalyze_config = config_section_map( "etanalyze-output", config_parser )
    main_config = config_section_map( "cycle-analyze", config_parser )
    edge_config = config_section_map( "edges", config_parser )
    edgeinfo_config = config_section_map( "edgeinfo", config_parser )
    objectinfo_config = config_section_map( "objectinfo", config_parser )
    summary_config = config_section_map( "summary_cpp", config_parser )
    return ( global_config, etanalyze_config, main_config, edge_config,
             edgeinfo_config, objectinfo_config, summary_config )

def main():
    parser = create_parser()
    args = parser.parse_args()
    configparser = ConfigParser.ConfigParser()
    assert( args.config != None )
    myconfig = process_config( args )
    # logging
    logger = setup_logger( filename = args.logfile,
                           debugflag = global_config["debug"] )
    #
    # Main processing
    #
    return main_process( debugflag = global_config["debug"],
                         output = args.output,
                         benchmark = args.benchmark,
                         lastedgeflag = args.lastedgeflag,
                         main_config = main_config,
                         etanalyze_config = etanalyze_config,
                         edge_config = edge_config,
                         edgeinfo_config = edgeinfo_config,
                         objectinfo_config = objectinfo_config,
                         summary_config = summary_config,
                         global_config = global_config,
                         logger = logger )

if __name__ == "__main__":
    main()
