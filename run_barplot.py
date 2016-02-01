import argparse
import os
import re
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument( "directory", help = "Target directory with CSV files." )

args = parser.parse_args()

def render_barplot( csvfile = None,
                    bmark = None,
                    title = None,
                    xlabel = None ):
    outpdf = "types.pdf"
    cmd = [ "/data/rveroy/bin/Rscript",
            "./barplot.R",
            csvfile, outpdf,
            bmark, title, xlabel ]
    print "Running barplot.R on %s -> %s" % (csvfile, outpdf)
    rproc = subprocess.Popen( cmd,
                              stdout = subprocess.PIPE,
                              stdin = subprocess.PIPE,
                              stderr = subprocess.PIPE )
    result = rproc.communicate()
    return result

csvre = re.compile( "([a-z0-9_]+)-basic_cycle_analyze-.*\.csv" )
for item in os.listdir( args.directory ):
    m = csvre.match(item)
    if m:
        bmark = m.group(1)
        title = "%s types" % bmark
        print "%s: %s" % (item, bmark)
        result = render_barplot( csvfile = item,
                                 bmark = bmark,
                                 title = title,
                                 xlabel = "type counts" )
