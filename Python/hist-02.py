#!/usr/bin/python
import sys
import os
import csv
from collections import Counter, defaultdict
from operator import itemgetter
import ntpath
import re
from pprint import PrettyPrinter
import subprocess
from tabulate import tabulate
from math import ceil

def setminmax(minmap, maxmap, key, v):
    minmap[key] = min(v, minmap[key])
    maxmap[key] = max(v, maxmap[key])

def render_graphs( rscript_path = None,
                   barplot_script = None,
                   textfile = None,
                   pdf_file = None,
                   png_file = None ):
    assert( os.path.isfile( rscript_path ) )
    assert( os.path.isfile( barplot_script ) )
    assert( os.path.isfile( textfile ) )
    cmd = [ rscript_path, # The Rscript executable
            barplot_script, # Our R script that generates the plots/graphs
            textfile, # The text file that contains the data
            pdf_file, # Output PDF file
            png_file, ] # Output PNG file
    print( "Running R barplot script  on %s -> (%s, %s)" % (textfile, pdf_file, png_file) )
    print( "[ %s ]" % str(cmd) )
    renderproc = subprocess.Popen( cmd,
                                   stdout = subprocess.PIPE,
                                   stdin = subprocess.PIPE,
                                   stderr = subprocess.PIPE )
    result = renderproc.communicate()
    # Send debug output to logger
    print("--------------------------------------------------------------------------------")
    for x in result:
        print(">>:", str(x))
    print("--------------------------------------------------------------------------------")

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
        self.total_size = 0
        self.site2set = defaultdict( set )

    def add(self, key, mysize, objs, age, byheap):
        self.size[key] += mysize
        self.objects[key] += objs
        self.groups[key] += 1

        setminmax(self.min_age, self.max_age, key, age)
        setminmax(self.min_group, self.max_group, key, objs)

        self.total_size += mysize
        if byheap:
            self.by_heap[key] += 1
        # else:
        #     # Actually now that I think about it, probably not necessary since
        #     # if a key has value zero if accessed on a read for the first time
        #     # in the Counter class.
        #     self.by_heap[key] += 0

    def update_site_count( self, key, tgtsite ):
        self.site2set[key].add( tgtsite )  

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

    def csv_write( self, writer ):
        header = [ "size", "objects", "groups", "min_group", "max_group",
                   "min_age", "max_age", "by_heap",
                   "allocsite", "deathsite",
                   "allocpackage", "deathpackage",
                   "alloc_same_death", ]
        writer.writerow( header )
        rows = []
        for key in self.size.keys():
            result = key[0].rsplit("/", 1)
            if len(result) > 1:
                allocpackage = result[0]
                allocsite = result[1]
            else:
                allocpackage = "None"
                allocsite = result[0]
            result = key[1].rsplit("/", 1)
            if len(result) > 1:
                deathpackage = result[0]
                deathsite = result[1]
            else:
                deathpackage = "None"
                deathsite = result[0]
            rows.append( [ self.size[key] / (1024*1024), # megabytes
                           self.objects[key],
                           self.groups[key],
                           self.min_group[key],
                           self.max_group[key],
                           self.min_age[key] / 1024, # kilobytes
                           self.max_age[key] / 1024, # kilobytes
                           self.by_heap[key],
                           allocsite,
                           deathsite,
                           allocpackage,
                           deathpackage,
                           str( ((allocsite == deathsite) and (allocpackage == deathpackage)) ) ] )
        rows = sorted( rows, key = itemgetter(0), reverse = True )
        for row in rows[:10]:
            writer.writerow( row )

    def samesite_latex_write( self, fptr ):
        header = [ "Site", "Objects", "Groups", "Size(kB)", ]
        alldata = sorted( self.objects.items(),
                          key = itemgetter(1),
                          reverse = True )
        index = 0
        data = []
        allrows = []
        print( "TOTAL SIZE:", self.total_size )
        while ( len(data) < 10 and
                index < len(alldata) ):
            rec = alldata[index]
            if rec[0] != "NOMATCH":
                data.append( rec[0] )
            index += 1
        assert( len(data) == 10 )
        fptr.write( "\\begin{table}\n    \\centering\n" )
        for site in data:
            row = [ site ]
            row.append( "{:,d}".format(self.objects[site]) )
            row.append( "{:,d}".format(self.groups[site]) )
            row.append( "{:,d}".format(ceil(self.size[site]/1024)) )
            allrows.append(row)
        fptr.write( tabulate( allrows, header, tablefmt = "latex", stralign = "right" ) )
        bmark_tex = bmark.replace("_", "\\_")
        fptr.write( "\n    \\caption{%s}\n    \\label{%s}\n" %
                    ( "Top contexts, using object count, where allocation site is the same as death site for %s. Size is in kilobytes." % bmark_tex,
                      bmark + "-group-table" ) )
        fptr.write( "\\end{table}\n" )

    def alloc_latex_write( self, fptr, bmark ):
        # Only valid for ALLOC type Clusters
        assert( self.name == "DEATH" )
        # ALLOC: header = [ "Benchmark", "Site", "Size(MB)", "Objects", "# of death sites", ]
        header = [ "Benchmark", "Site", "Size(MB)", "Objects", "# of allocation sites", ]
        alldata = sorted( self.size.items(),
                          key = itemgetter(1),
                          reverse = True )
        data = []
        allrows = []
        print( "TOTAL SIZE:", self.total_size )
        index = 0
        while len(data) < 1:
            rec = alldata[index]
            if rec[0] != "NOMATCH":
                data.append( rec[0] )
                break
            index += 1
        assert( len(data) == 1 )
        fptr.write( "\\begin{table*}\n    \\centering\n" )
        bmark_tex = bmark.replace("_", "\\_")
        for site in data:
            row = [ bmark_tex, site ]
            row.append( "{:,d}".format(ceil(self.size[site]/(1024*1024))) )
            row.append( "{:,d}".format(self.objects[site]) )
            row.append( "{:,d}".format(len(self.site2set[site])) )
            allrows.append(row)
        fptr.write( tabulate( allrows, header, tablefmt = "latex", stralign = "right" ) )
        fptr.write( "\n    \\caption{%s}\n    \\label{%s}\n" %
                    ( "Benchmark's top allocation site by size.",
                      bmark + "-all-alloc-table" ) )
        # DEATH: fptr.write( "\n    \\caption{%s}\n    \\label{%s}\n" %
        # DEATH:             ( "Benchmark's top death site by size.",
        # DEATH:               bmark + "-all-death-table" ) )
        fptr.write( "\\end{table*}\n" )

if __name__ == "__main__":
    try:
        csvfile = sys.argv[1] if not subprocflag else mytarget
    except:
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
    by_samesite = ClusterIndex("SAMESITE")

    groupsize = Counter()

    pp = PrettyPrinter( indent = 4 )
    gcount_file = bmark + "-GROUP-COUNT.txt"
    site_texfile = bmark + "-SITES.tex"
    # alloc_texfile = bmark + "-ALLOC.tex"
    #    open( alloc_texfile, "w" ) as allocTexFile:
    # open( bmark + ".txt", "w" ) as outFile, \
    # open( gcount_file, "w" ) as gcountFile,\
    # open( site_texfile, "w" ) as siteTexFile, \
    # open( death_texfile, "w" ) as deathTexFile:
    death_texfile = bmark + "-DEATH.tex"
    dset = set()
    tmpsize = 0
    tmpobjs = 0
    with open( csvfile, "r" ) as theFile:
        reader = csv.DictReader( theFile )
        for line in reader:
            size = int(line["size-group"])
            objs = int(line["number-objects"])
            oldest = int(line["oldest-member-age"])

            context = line["non-Java-lib-context"] if (line["non-Java-lib-context"] != "NONE") \
                else line["death-context-1"]

            by_heap = (line["pointed-at-by-heap"] == "True")
            alloc = line["alloc-non-Java-lib-context"]
            # if alloc == "org/apache/lucene/queryParser/QueryParser.parse":
            if context == "org/apache/lucene/queryParser/QueryParserTokenManager.ReInit":
                dset.add( alloc )
                tmpsize += size
                tmpobjs += objs
            # by_allocsite.add(alloc, size, objs, oldest, by_heap)
            # by_allocsite.update_site_count( alloc, context )
            # by_deathsite.add(context, size, objs, oldest, by_heap)
            # by_deathsite.update_site_count( context, alloc )
            # TODO TEMP: by_type.add(line["key-type"], size, objs, oldest, by_heap)

            # TODO TEMP: k = '{} {} {}'.format(line["key-type"], alloc, context)
            # TODO TEMP: by_all.add(k, size, objs, oldest, by_heap)
            # TODO TEMP: by_contpair.add( (alloc, context), size, objs, oldest, by_heap )

            # SAMESITE: samesite_flag = (alloc == context)
            # SAMESITE: site_to_add = alloc if samesite_flag else "NOMATCH"
            # SAMESITE: by_samesite.add( site_to_add, size, objs, oldest, by_heap )

            # Size histograms
            # TODO TEMP: groupsize[objs] += 1

            # Checking to see if alloc site is the same as the death site
        print( len(dset), tmpsize, tmpobjs )
        # TODO TEMP: outFile.write( "{}     {:8s} {:8s} {:8s} {:11s}  {:22s} {:8s} {}\n".format("TAG", "#bytes", "#objs", "#clusters", "cl-size", "cluster-age", "by-heap", "info"))
        # ORIG: by_allocsite.print_file( outFile )
        # ORIG: by_deathsite.print_file( outFile )
        # ALLOC: by_allocsite.alloc_latex_write( allocTexFile, bmark )
        # DEATH: by_deathsite.alloc_latex_write( deathTexFile, bmark )
        # ORIG: by_type.print_file( outFile )
        # ORIG: by_all.print_file( outFile )
        # GROUP SIZE: for num, count in groupsize.items():
        # GROUP SIZE:     for i in range(count):
        # GROUP SIZE:         gcountFile.write("%d\n" % num)
        # SAMESITE: by_samesite.samesite_latex_write( siteTexFile )

    # TODO TEMP: with open( "AD-" + bmark + ".csv", "w" ) as fptr:
    # TODO TEMP:     writer = csv.writer(fptr, quoting = csv.QUOTE_NONNUMERIC )
    # TODO TEMP:     by_contpair.csv_write( writer )

    # TODO TEMP: with open( bmark + ".csv", "w" ) as fptr:
    # TODO TEMP:     writer = csv.writer(fptr, quoting = csv.QUOTE_NONNUMERIC )
    # TODO TEMP:     by_contpair.csv_write( writer )

    # TODO TEMP: latex_file = bmark + "-GROUP-COUNT.tex"
    # TODO TEMP: with open( latex_file, "w" ) as fptr:
    # TODO TEMP:     data = sorted( groupsize.items(), key = itemgetter(0) )
    # TODO TEMP:     header = [ str(x[0]) for x in data ]
    # TODO TEMP:     row = [ "{:,d}".format(x[1]) for x in data ]
    # TODO TEMP:     fptr.write( "\\begin{table}\n    \\centering\n" )
    # TODO TEMP:     fptr.write( tabulate( [ row ], header, tablefmt = "latex", stralign = "right" ) )
    # TODO TEMP:     bmark_tex = bmark.replace("_", "\\_")
    # TODO TEMP:     fptr.write( "\n    \\caption{%s}\n    \\label{%s}\n" %
    # TODO TEMP:                 ("Group size frequency for " + bmark_tex, bmark + "-group-table") )
    # TODO TEMP:     fptr.write( "\\end{table}\n" )
    # GROUP SIZE: pdf_file = bmark + "-GROUP-COUNT.pdf"
    # GROUP SIZE: png_file = bmark + "-GROUP-COUNT.png"
    # GROUP SIZE: render_graphs( rscript_path = "/data/rveroy/bin/Rscript",
    # GROUP SIZE:                barplot_script = "raw-plot.R",
    # GROUP SIZE:                textfile = gcount_file, # csvfile is the input from the output_summary earlier 
    # GROUP SIZE:                pdf_file = pdf_file,
    # GROUP SIZE:                png_file = png_file )