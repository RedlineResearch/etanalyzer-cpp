#!/usr/bin/python
import sys
import csv
from collections import Counter, defaultdict
from operator import itemgetter
import ntpath
import re

def setminmax(minmap, maxmap, key, v):
    minmap[key] = min(v, minmap[key])
    maxmap[key] = max(v, maxmap[key])


class ClusterIndex:
    def __init__(self, name):
        self.name = name
        # Counter and defaultdict makes the rest of the code easier to read
        self.size = Counter()
        self.objects = Counter()
        self.groups = Counter()
        self.min_group = defaultdict( lambda: sys.maxsize )
        self.max_group = defaultdict( lambda: -1 )
        self.min_age = defaultdict( lambda: sys.maxsize )
        self.max_age = defaultdict( lambda: -1 )
        self.by_heap = Counter()

    def add(self, key, size, objs, age, byheap):
        self.size[key] += size
        self.objects[key] += objs
        self.groups[key] += 1

        setminmax(self.min_age, self.max_age, key, age)
        setminmax(self.min_group, self.max_group, key, objs)

        if byheap:
            self.by_heap[key] += 1
        # else:
        #     # Actually now that I think about it, probably not necessary since
        #     # if a key has value zero if accessed on a read for the first time
        #     # in the Counter class.
        #     self.by_heap[key] += 0

    def print(self):
        for key in self.size:
            print( '{} {:8d} {:8d} {:8d} {:3d} -- {:4d}  {:9d} -- {:9d} {:8d} {}'.format(self.name, self.size[key], self.objects[key], self.groups[key], self.min_group[key], self.max_group[key], self.min_age[key], self.max_age[key], self.by_heap[key], key))

    def print_file(self, fptr):
        for key in self.size:
            fptr.write( "{} {:8d} {:8d} {:8d} {:3d} -- {:4d}  {:9d} -- {:9d} {:8d} {}\n".format(self.name, self.size[key], self.objects[key], self.groups[key], self.min_group[key], self.max_group[key], self.min_age[key], self.max_age[key], self.by_heap[key], key))

    def print_file_limit(self, fptr, number):
        rows = []
        for key in self.size.keys():
            rows.append( [ self.size[key],
                           self.objects[key],
                           self.groups[key],
                           self.min_group[key],
                           self.max_group[key],
                           self.min_age[key],
                           self.max_age[key],
                           self.by_heap[key],
                           key ] )
        rows = sorted( rows, key = itemgetter(0), reverse = True )
        assert( number > 0 and number <= len(rows) )
        for row in rows[:number]:
            fptr.write( "{:8d} {:8d} {:8d} {:3d} -- {:4d}  {:9d} -- {:9d} {:8d} {}\n".format( row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], str(key)) )

    def csv_write( self, writer, name ):
        header = [ "size", "objects", "groups", "min_group", "max_group", "min_age", "max_age", "by_heap", "allocsite", "deathsite" ]
        writer.writerow( header )
        rows = []
        for key in self.size.keys():
            rows.append( [ self.size[key],
                           self.objects[key],
                           self.groups[key],
                           self.min_group[key],
                           self.max_group[key],
                           self.min_age[key],
                           self.max_age[key],
                           self.by_heap[key],
                           key[0],
                           key[1] ] )
        rows = sorted( rows, key = itemgetter(0), reverse = True )
        for row in rows[:10]:
            writer.writerow( row )

if __name__ == "__main__":
    csvfile = sys.argv[1]

    # Hardcoded to simplify filename generation. Could be changed if needed.
    regex = re.compile( "([a-z0-9_]+)-raw-key-object-summary.csv" )
    m = regex.search( ntpath.basename(csvfile) )
    if m:
        bmark = m.group(1)
    else:
        print( "Unable to extract benchmark name from arg: %s" % csvfile )
        exit(1)
    by_allocsite = ClusterIndex("ALLOC")
    by_deathsite = ClusterIndex("DEATH")
    by_type = ClusterIndex("TYPE")
    by_all = ClusterIndex("ADT")
    by_contpair = ClusterIndex("CONTPAIR")

    with open( csvfile, "r" ) as theFile, \
         open( bmark + ".txt", "w" ) as outFile:
        reader = csv.DictReader( theFile )
        for line in reader:
            size = int(line["size-group"])
            objs = int(line["number-objects"])
            oldest = int(line["oldest-member-age"])

            context = line["non-Java-lib-context"] if (line["non-Java-lib-context"] != "NONE") \
                else line["death-context-1"]

            by_heap = (line["pointed-at-by-heap"] == "True")
            alloc = line["alloc-non-Java-lib-context"]
            by_allocsite.add(alloc, size, objs, oldest, by_heap)
            by_deathsite.add(context, size, objs, oldest, by_heap)
            by_type.add(line["key-type"], size, objs, oldest, by_heap)

            k = '{} {} {}'.format(line["key-type"], alloc, context)
            by_all.add(k, size, objs, oldest, by_heap)

            by_contpair.add( (alloc, context), size, objs, oldest, by_heap )

        outFile.write( "{}     {:8s} {:8s} {:8s} {:11s}  {:22s} {:8s} {}\n".format("TAG", "#bytes", "#objs", "#clusters", "cl-size", "cluster-age", "by-heap", "info"))
        by_allocsite.print_file( outFile )
        by_deathsite.print_file( outFile )
        by_type.print_file( outFile )
        by_all.print_file( outFile )

    with open( "AD-" + bmark + ".csv", "w" ) as fptr, \
         open( "AD-" + bmark + ".txt", "w" ) as textfptr:
        writer = csv.writer(fptr, quoting = csv.QUOTE_NONNUMERIC )
        by_contpair.csv_write( writer, "AD" )
        by_contpair.print_file_limit( textfptr, 10 )

