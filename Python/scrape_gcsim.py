from __future__ import division

import os
from glob import glob
import csv

filelist = glob( "*.txt" )
print "Number of files: %d" % len(filelist)

# TODO replace the hard coded benchmark and filename with a commmand line parameter
with open("specjbb-sim.csv", "wb") as csvfptr:
    writer = csv.writer( csvfptr, quoting = csv.QUOTE_NONE )
    # print "'RUN','Heap_size','Num_collections','mark_deferred','mark_saved','total_alloc','mark_regular','markcons_def','markcons_reg'"
    header = [ "RUN",
               "Heap_size",
               "Num_collections",
               "mark_deferred",
               "mark_saved",
               "total_alloc",
               "mark_regular",
               "markcons_def",
               "markcons_reg", ]
    writer.writerow( header )
    for ind in xrange(1, len(filelist) + 1):
        fname = "specjbb-simulator-GC-%d.txt" % ind
        with open(fname, "rb") as fptr:
            row = [ ind, ]
            data = fptr.readlines()
            pad1, key, value, pad2 = data[0].rstrip().split(" ")
            memsize = int(value)
            row.append( memsize )
            for line in data[-4:]:
                line = line.rstrip()
                key, value = line.split(":")
                # print "[%s] => %s" % (key, value)
                row.append( int(value) )
            num_collections = row[2]
            mark_def = row[3]
            mark_saved = row[4]
            total_alloc = row[5]
            mark_regular = mark_def + mark_saved
            markcons_def = mark_def / total_alloc
            markcons_regular = mark_regular / total_alloc
            # Marks for non-deferred regular collection
            row.append( mark_regular )
            # Mark con ratio deferred
            row.append( "{:.2f}".format(markcons_def) )
            # Mark con ratio regular
            row.append( "{:.2f}".format(markcons_regular) )
            writer.writerow( row )
            # print row
