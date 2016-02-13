import argparse
import os
import sys
import re
from collections import Counter
import pprint
import string

def create_parser():
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "infile", help = "Target input filename." )
    return parser

# Example line:
#   S> OBJ 0x53(Ljava/lang/Class; <NONE> @0) : 817102936
def process_line( line ):
    fields = line.split()
    part = fields[2].split("(")
    myclass = part[1]
    # objId = part[0]
    # deathsite = fields[3]
    # alloctime = fields[4]
    # deathtime = fields[6]
    return myclass

def is_blacklisted( mytype ):
    return ( mytype == "[B" or
             mytype == "[C" or
             mytype == "[I" or
             mytype == "[J" or
             mytype == "[F" or
             mytype[0:11] == "Ljava/lang/" or
             mytype[0:12] == "[Ljava/lang/" or
             (string.find(mytype, "java/lang") >= 0 ) )

def main( infile = None ):
    pp = pprint.PrettyPrinter( indent = 4 )
    Hcount = Counter()
    Scount = Counter()
    with open( infile, "rb" ) as fptr:
        sre = re.compile( "^S>" )
        hre = re.compile( "^H>" )
        for line in fptr:
            line = line.rstrip()
            mS = sre.match(line)
            mH = hre.match(line)
            if mS or mH:
                myclass = process_line( line )
                if not is_blacklisted(myclass):
                    if mS:
                        Scount.update( [ myclass ] )
                    else:
                        Hcount.update( [ myclass ] )
    print "=======[ STACK ]================================================================"
    mc = Scount.most_common(30)
    pp.pprint(mc)
    print "--------------------------------------------------------------------------------"
    for key, val in Scount.iteritems():
        print "%s -> %d" % (key, val)
    print "=======[ HEAP ]================================================================="
    mc = Hcount.most_common(30)
    pp.pprint(mc)
    print "--------------------------------------------------------------------------------"
    for key, val in Hcount.iteritems():
        print "%s -> %d" % (key, val)

if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    #
    # Main processing
    #
    main( infile = args.infile )
