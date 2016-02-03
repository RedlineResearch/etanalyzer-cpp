import argparse
import os
import re
import multiprocessing
from multiprocessing import Process

parser = argparse.ArgumentParser()
parser.add_argument( "directory", help = "Target work directory." )

args = parser.parse_args()

def test1( start, stop ):
    with open( str(stop) + ".txt", "wb" ) as fp:
        for x in xrange(start, stop):
            fp.write( "%d\n" % x )

def main():
    # for item in os.listdir( args.directory ):
    #     pass
    p1 = Process( target = test1,
                  args = (0,9999999) )
    p2 = Process( target = test1,
                  args = (100000, 10000000) )
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    

if __name__ == "__main__":
    main()
