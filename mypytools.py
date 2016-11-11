# mypytools.py
# - Raoul L. Veroy
import math
import smtplib
# from email.mime.text import MIMEText
import subprocess
import StringIO

# for get_file_fp
import re
import subprocess
import os

# For timestamping directories and files.
from datetime import datetime, date
import time

# Defaultdicts are better than standard dictionaries!
from collections import defaultdict

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def as_sequence(iterable):
    if isinstance( iterable, (list, tuple) ):
        return iterable
    else:
        return list(iterable)

# From http://code.activestate.com/recipes/393090/
def add_partial(x, partials):
    # Rounded x + y stored in hi with the round-off stored in lo.  Together
    # hi+lo are exactly equal to x+y. 
    # Depends on IEEE-754 arithmetic guarantees. See proof of correctness at:
    # www-2.cs.cmu.edu/afs/cs/project/quake/public/papers/robust-arithmetic.ps
    i = 0
    for y in partials:
        if abs(x) < abs(y):
            x, y = y, x
        hi = x + y
        lo = y - (hi - x)
        if lo:
            partials[i] = lo
            i += 1
        x = hi
    partials[i:] = [x]

def _generalised_sum( data, func ):
    """
    >>> _generalised_sum( [1.0, 2.0, 3.0, 4.0], None )
    (4, 10.0)
    """
    try:
        count = len(data)
    except TypeError:
        # Iterables without len.
        partials = []
        count = 0
        if func is None:
            for count, x in enumerate(data, 1):
                add_partial(x, partials)
        else:
            for count, x in enumerate(data, 1):
                add_partial(func(x), partials)
        total = math.fsum(partials)
    else:
        if func is None:
            total = math.fsum(data)
        else:
            total = math.fsum(func(x) for x in data)
    return (count, total)

def mean( data ):
    """Return the sample arithmetic mean of a sequence of numbers.

    >>> mean([1.0, 2.0, 3.0, 4.0])
    2.5
    """
    n, total = _generalised_sum(data, None)
    if n == 0:
        raise ValueError('mean of empty sequence is not defined')
    return total / n

def _SS( data, m ):
    if m is None:
        m = mean( as_sequence(data) )
    return _generalised_sum( data, lambda x: (x - m)**2 )

def variance(data, m = None):
    """
    >>> variance( [ 0.25, 0.5, 1.25, 1.25, 1.75, 2.75, 3.5 ] ) #doctest: +ELLIPSIS
    1.37202380952...
    """
    n, ss = _SS(data, m)
    if n < 2:
        raise ValueError( 'Variance or standard deviation requires at least two data points' )
    return ss / (n - 1)

def stdev( data, m = None ):
    """
    >>> stdev([1.5, 2.5, 2.5, 2.75, 3.25, 4.75]) #doctest: +ELLIPSIS
    1.08108741552...
    >>> stdev([1.5, 2.5, 2.75, 2.75, 3.25, 4.25], 3) #doctest: +ELLIPSIS
    0.921954445729...
    """
    return math.sqrt( variance(data, m) )

def email_message( message = "",
                   subject = "",
                   to_email = "rveroy@cs.tufts.edu",
                   mutt_path = "/usr/bin/mutt" ):
    """ Send an email message using mutt.
    """
    # minor TODO: As this obviously relies on mutt, it's not very portable.
    # I can't seem to figure out the python smtplib library so that's a good
    # portable alternative.
    msg = StringIO.StringIO( message )
    cmd = [ mutt_path, "-s %s" % subject, "--", "%s" % to_email ]
    proc = subprocess.Popen( cmd,
                             stdin = subprocess.PIPE,
                             stdout = subprocess.PIPE,
                             stderr = subprocess.PIPE )
    result = proc.communicate( message )
    return result

def XXXX_email_message( message = "",
                        subject = "",
                        from_email = "rveroy@cs.tufts.edu",
                        to_email = "rveroy@cs.tufts.edu",
                        smtp_host = "smtp-tls.eecs.tufts.edu",
                        smtp_port = 465 ):
    """ Send an email message.
    """
    msg = ( "From: %s\r\nTo: %s\r\n\r\n"
            % (from_email, ", ".join( [ to_email ] )) )
    # msg = MIMEText( message )
    # msg["Subject"] = subject
    # msg["From"] = from_email
    # msg["To"] = to_email
    server = smtplib.SMTP( host = smtp_host,
                           port = smtp_port )
    # server.send( from_email, [ to_email ], msg.as_string() )
    server.send( from_email, [ to_email ], msg )
    server.quit()

def get_file_fp( myfile = None,
                 logger = None ):
    if not os.path.isfile( myfile ) and not os.path.islink( myfile ):
        # File does not exist
        if logger != None:
            logger.error( "Unable to open %s" % str(myfile) )
        print "Unable to open %s" % str(myfile)
        exit(21)
    bz2re = re.compile( "(.*)\.bz2$", re.IGNORECASE )
    gzre = re.compile( "(.*)\.gz$", re.IGNORECASE )
    bz2match = bz2re.search( myfile )
    gzmatch = gzre.search( myfile )
    if bz2match: 
        # bzip2 file
        fp = subprocess.Popen( [ "bzcat", myfile ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    elif gzmatch: 
        # gz file
        fp = subprocess.Popen( [ "zcat", myfile ],
                               stdout = subprocess.PIPE,
                               stderr = subprocess.PIPE ).stdout
    else:
        fp = open( myfile, "r")
    return fp

# TODO TODO TODO 
# HERE TODO TODO
# TODO TODO TODO 
# TODO: Import time/date related libraries
def create_work_directory( work_dir,
                           today = "",
                           timenow = "",
                           logger = None,
                           interactive = False ):
    os.chdir( work_dir )
    # Check today directory ---------------------------------------------------
    if os.path.isfile(today):
        if logger != None:
            pass # TODO
        else:
            print "Can not create %s as directory." % today
        exit(11)
    if not os.path.isdir( today ):
        os.mkdir( today )
    else:
        if logger != None:
            logger.warning( "WARNING: %s directory exists." % today )
        else:
            print "WARNING: %s directory exists." % today
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    os.chdir( today )
    # Check timenow directory -------------------------------------------------
    if os.path.isfile(timenow):
        if logger != None:
            pass # TODO
        else:
            print "Can not create %s as directory." % timenow
        exit(11)
    if not os.path.isdir( timenow ):
        os.mkdir( timenow )
    else:
        if logger != None:
            logger.warning( "WARNING: %s directory exists." % timenow )
        else:
            print "WARNING: %s directory exists." % timenow
        if interactive:
            raw_input("Press ENTER to continue:")
        else:
            print "....continuing!!!"
    os.chdir( timenow )
    return str(os.getcwd())

def check_host( benchmark = None,
                hostlist = [],
                host_config = {} ):
    import socket
    assert( type(hostlist) == type([]) )
    assert( type(host_config) == type({}) or
            type(host_config) == type(defaultdict()) )
    thishost = socket.gethostname()
    for wanthost in hostlist:
        actual = get_actual_hostname( hostname = wanthost,
                                      host_config = host_config )
        if thishost in host_config[actual]:
            return True
    return False

def process_host_config( host_config = {} ):
    for host in list(host_config.keys()):
        hostlist = host_config[host].split(",")
        hostlist.append(host)
        host_config[host] = hostlist
    print "***: %s" % str(host_config)
    return defaultdict( list, host_config )

def get_actual_hostname( hostname = "",
                         host_config = {} ):
    for key, hlist in host_config.iteritems():
        if hostname in hlist:
            return key
    return None

def process_worklist_config( worklist_config = {} ):
    mydict = defaultdict( lambda: "NONE" )
    for bmark in list(worklist_config.keys()):
        hostlist = worklist_config[bmark].split(",")
        mydict[bmark] = hostlist
    return mydict

def is_specjvm( bmark ):
    print "================================================================================"
    print "  TODO: Fix to dynamically populate is_***** benchmark functions from"
    print "        configuration files."
    print "================================================================================"
    return ( bmark == "_201_compress" or
             bmark == "_202_jess" or
             bmark == "_205_raytrace" or
             bmark == "_209_db" or
             bmark == "_213_javac" or
             bmark == "_222_mpegaudio" or
             bmark == "_227_mtrt" or
             bmark == "_228_jack" )

def is_dacapo( bmark ):
    print "================================================================================"
    print "  TODO: Fix to dynamically populate is_***** benchmark functions from"
    print "        configuration files."
    print "================================================================================"
    return bmark in [ "avrora", "batik", "eclipse", "fop", "h2", "jython",
                      "luindex", "lusearch", "pmd", "specjbb", "sunflow",
                      "tomcat", "tradebeans", "tradesoap", "xalan", ]

def is_minibench( bmark ):
    print "================================================================================"
    print "  TODO: Fix to dynamically populate is_***** benchmark functions from"
    print "        configuration files."
    print "================================================================================"
    return bmark in [ "seq-seqdel", "seq-enddel", "rand-seqdel", "rand-enddel", "stackonly",
                      "seq-use", "seq-dontuse", "static-use", "static-dontuse",
                      "stable_example", ]


__all__ = [ "mean", "merge_two_dicts", "stdev", "variance", "email_message",
            "get_file_fp", "check_host", "get_actual_hostname", "process_host_config",
            "process_worklist_config",
            "is_specjvm", "is_dacapo", "is_minibench", ]

if __name__ == "__main__":
    import doctest
    doctest.testmod()

