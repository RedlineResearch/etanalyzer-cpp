import os
import sys
import logging
import atexit
from time import clock
import re
import sqlite3
from optparse import OptionParser
import sqlite3tools
import pprint

# Setup logging
logger = None
logger_name = 'readintodb'
def setup_logger( targetdir = ".",
                  filename = "readintodb.log",
                  debugflag = 0 ):
    global logger
    global logger_name
    print "logger : ", filename
    # Set up main logger
    logger = logging.getLogger( logger_name )
    formatter = logging.Formatter( '[%(module)s] %(funcName)s : %(message)s' )
    logger.setLevel( logging.DEBUG )
    filehandler = logging.FileHandler( os.path.join( targetdir, filename ) , 'w' )
    filehandler.setLevel( logging.DEBUG )
    filehandler.setFormatter( formatter )
    logger.addHandler( filehandler )
    if debugflag:
        chandler = logging.StreamHandler()
        chandler.setLevel( logging.DEBUG )
        chandler.setFormatter( formatter )
        logger.addHandler( chandler )

def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

line = "="*40 + '\n'
def log(s, elapsed=None):
    global logger
    logger.debug( line )
    logger.debug( secondsToStr(clock()) + '-' + s )
    if elapsed:
        logger.debug( "Elapsed time: %s" % elapsed )
    logger.debug( line )

start = clock()
def endlog():
    end = clock()
    elapsed = end - start
    log("End Program", secondsToStr(elapsed))

def now():
    return secondsToStr(clock())

def create_db_ver_1( conn = None,
                       droptable_flag = False ):
    cur = conn.cursor()
    if droptable_flag:
        cur.execute( '''DROP TABLE IF EXISTS trace''' )
        cur.execute( '''DROP TABLE IF EXISTS attributes''' )
    cur.execute( """CREATE TABLE trace
                 (recid INTEGER PRIMARY KEY,
                  op TEXT,
                  objid TEXT,
                  threadid TEXT,
                  methodid TEXT)""" )
    cur.execute( """CREATE TABLE attributes (key text, value text)""" )
    cur.execute( "INSERT INTO attributes VALUES ('version', '1')" )
    conn.execute( 'DROP INDEX IF EXISTS idx_trace_recid' )
    conn.execute( 'DROP INDEX IF EXISTS idx_trace_op' )

def create_db_ver_3( conn = None,
                     droptable_flag = False ):
    cur = conn.cursor()
    if droptable_flag:
        cur.execute( '''DROP TABLE IF EXISTS trace''' )
        cur.execute( '''DROP TABLE IF EXISTS attributes''' )
    cur.execute( """CREATE TABLE trace
                 (recid INTEGER PRIMARY KEY,
                  op TEXT,
                  field1 TEXT,
                  field2 TEXT,
                  field3 TEXT,
                  field4 TEXT)""" )
    cur.execute( """CREATE TABLE attributes (key text, value text)""" )
    cur.execute( "INSERT INTO attributes VALUES ('version', '3')" )
    conn.execute( 'DROP INDEX IF EXISTS idx_trace_recid' )
    conn.execute( 'DROP INDEX IF EXISTS idx_trace_op' )

def create_db_ver_3_debug( conn = None,
                           droptable_flag = False ):
    cur = conn.cursor()
    if droptable_flag:
        cur.execute( '''DROP TABLE IF EXISTS trace''' )
        cur.execute( '''DROP TABLE IF EXISTS attributes''' )
    cur.execute( """CREATE TABLE trace
                 (recid INTEGER PRIMARY KEY,
                  op TEXT,
                  field1 TEXT)""" )
    cur.execute( """CREATE TABLE attributes (key text, value text)""" )
    cur.execute( "INSERT INTO attributes VALUES ('version', '3')" )
    conn.execute( 'DROP INDEX IF EXISTS idx_trace_recid' )
    conn.execute( 'DROP INDEX IF EXISTS idx_trace_op' )

def create_db( conn = None,
               droptable_flag = False,
               version = 3 ):
    if version == 1:
        print "Version 1:",
        create_db_ver_1( conn = conn,
                           droptable_flag = droptable_flag )
    elif version == 3:
        print "Version 3 DEBUG:",
        # create_db_ver_3( conn = conn,
        #                   droptable_flag = droptable_flag )
        create_db_ver_3_debug( conn = conn,
                               droptable_flag = droptable_flag )
    else:
        print "Unknown version number."
        exit(2)
    conn.commit()
    conn.text_factory = str

# TODO get rid of version check
def parse_line( line = None,
                version = None ):
    assert( line != None )
    assert( version != None )
    tup = None
    if version == 1:
        a = line.split()
        # logger.debug( "%d fields    type: %s" % (len(a), str(type(a))) )
        if a[0] == "A":
            tup = (a[0], a[1], a[4], None) 
        elif a[0] == "D":
            tup = (a[0], a[1], None, None) 
        elif a[0] == "U":
            tup = (a[0], a[2], a[4], None) 
        elif a[0] == "M":
            tup = (a[0], a[2], a[3], a[1]) 
        elif a[0] == "E":
            tup = (a[0], a[2], a[3], a[1]) 
        elif a[0] == "R":
            tup = (a[0], a[2], None, None)
        else:
            # TODO
            # Should recover gracefully. Not too difficult.
            # Just log an error and keep chugging on.
            # TODO
            assert(0)
    elif version == 3:
        a = line.split()
        # logger.debug( "%d fields    type: %s" % (len(a), str(type(a))) )
        if a[0] == "A" or a[0] == "U":
            tup = (a[0], a[1], a[2], a[3], a[4])
        elif a[0] == "D":
            tup = (a[0], a[1], None, None, None) 
        elif a[0] == "M" or a[0] == "E":
            tup = (a[0], a[1], a[2], a[3], None) 
        elif a[0] == "R":
            tup = (a[0], a[1], a[2], None, None)
        else:
            # TODO
            # Should recover gracefully. Not too difficult.
            # Just log an error and keep chugging on.
            # TODO
            assert(0)
    else:
        # TODO Log as an error
        print "Unknown version: ", version
        print "Exiting."
        exit(3)
    return tup

def read_input_into_db( conn = None,
                        infile = None,
                        version = None ):
    global logger
    assert( conn != None )
    assert( logger != None )
    assert( infile != None )
    assert( version != None )
    # bufsize = 4 * 1073741824 # TODO this is 4 gigs.
    cur = conn.cursor()
    # Open the input file
    fptr = open( infile, 'r' )
    total = 0
    if version == 1:
        mycmd = 'INSERT INTO trace VALUES (NULL,?,?,?,?)'
    elif version == 3:
        mycmd = 'INSERT INTO trace VALUES (NULL,?,?,?,?,?)'
    else:
        # TODO
        print "Unknown version", version
        exit(4)
    mycmd = 'INSERT INTO trace VALUES (NULL,?,?)'
    for line in fptr:
        # TODO
        tup = parse_line( line = line, version = version )
        # TODO TODO TODO cur.execute( mycmd, tup )
        cur.execute( mycmd, (tup[0], tup[1]) )
        total = total + 1
        if (total % 10000) == 0:
             sys.stderr.write( "." )
    sys.stderr.write( '\n' )
    conn.commit()
    print total, "lines read in."
    fptr.close()

def main_process( infile = None,
                  outdb = None,
                  version = 3,
                  droptable_flag = False,
                  debugflag = False ):
    global logger
    global conn
    try:
        conn = sqlite3.connect( outdb )
    except:
        logger.critical( "Unable to open %s" % outdb )
        print "Unable to open %s" % outdb
        exit(1)
    create_db( conn = conn,
               droptable_flag = droptable_flag,
               version = version )
    read_input_into_db( conn = conn,
                        infile = infile,
                        version = version )
    conn.execute( 'CREATE UNIQUE INDEX idx_trace_recid ON trace (recid)' )
    conn.execute( 'CREATE INDEX idx_trace_op ON trace (op)' )
    conn.commit()
    conn.close()

def main():
    global logger
    # initialize path variables
    # process options
    usage = "usage: %prog [options]"
    parser = OptionParser( usage=usage )
    parser.set_defaults( infile = None,
                         outdbfile = None,
                         version = 1,
                         droptable_flag = False,
                         debug = 0 )
    parser.add_option( "--infile",
                       action = "store",
                       dest = "infile",
                       metavar = "INFILE",
                       help = "Set name of input Elephant Trace file." )
    parser.add_option( "--outdb",
                       action = "store",
                       dest = "outdb",
                       metavar = "OUTDB",
                       help = "set output DB filename" )
    parser.add_option( "--version",
                       action = "store",
                       dest = "version",
                       metavar = "VERSION",
                       help = "Set the version of the DB:\n(Avail: 0.1)." )
    # droptable if it exists
    parser.add_option( "--droptable",
                       action = "store_true",
                       dest = "droptable_flag",
                       metavar = "DROPTABLE",
                       help = "Drop the db table if it exists." )
    # start timer if flag is enabled
    parser.add_option( "--timer",
                       action = "store_true",
                       dest = "timerflag",
                       metavar = "TIMER",
                       help = "time the parser" )
    # set logging to debug level
    parser.add_option( "--debug",
                       action = "store_true",
                       dest = "debugflag",
                       metavar = "DEBUG",
                       help = "increase the debug levels" )
    (options, args) = parser.parse_args()
    if len( args ) != 0:
        parser.error( "incorrect number of arguments. (Should be zero!)" )
    #
    # Get input filename
    #
    infile = options.infile
    if not infile:
        parser.error( "you must set a infile using --infile." )
    try:
        if not os.path.exists( infile ):
            parser.error( infile + " does not exist." )
    except:
        parser.error( "invalid path name : " + infile )
    #
    # Get outdb
    #
    outdb = options.outdb
    if not outdb:
        parser.error( "you must set a outdb using --outdb" )
    elif not re.match( "[a-zA-Z0-9\._]+$", outdb ):
        parser.error( "Invalid outdb: " + outdb )
    # Actually open the db in main_process()
    # 
    # Get version
    version = options.version
    try:
        tmpver = int(version)
        version = tmpver
    except:
        parser.error( "Illegal version", version )
    print "XXX: version ", version
    # set droptable_flag
    droptable_flag = options.droptable_flag
    # set timer flag
    timerflag = options.timerflag
    # set debug flag
    debugflag = options.debugflag
    setup_logger( filename = outdb + ".log" )
    # set timer flag
    timerflag = options.timerflag
    if timerflag:
        atexit.register( endlog )
        log("Start Program")
    #
    # Main processing
    #
    return main_process( infile = infile,
                         outdb = outdb,
                         version = version,
                         droptable_flag = droptable_flag,
                         debugflag = debugflag )

if __name__ == "__main__":
    main()
