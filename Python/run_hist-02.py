import subprocess

# Run hist-02.py for the following benchmarks

benchmarks = [ "_201_compress",
               "_202_jess",
               "_205_raytrace",
               "_209_db",
               "_213_javac",
               "_222_mpegaudio",
               "_227_mtrt",
               "_228_jack",
               "avrora",
               "batik",
               "fop",
               "luindex",
               "lusearch",
               "specjbb",
               "tomcat",
               "xalan", ]

def exec_full( filepath, bmark ):
    import os
    global_namespace = {
        "__file__" : filepath,
        "__name__" : "__main__",
        "mytarget" : bmark,
        "subprocflag" : True,
    }
    with open(filepath, 'rb') as fptr:
        exec( compile( fptr.read(), filepath, 'exec' ),
              global_namespace )

for bmark in benchmarks:
    print( "Running for %s" % bmark )
    # execute the file
    rawfile = bmark + "-raw-key-object-summary.csv"
    # exec_full( "./test.py", rawfile )
    exec_full( "./hist-02.py", rawfile )
    print( "====[ DONE: %s ]============================================================" % bmark )
