import os
import os.path

for myfile in os.listdir("."):
    newstr = myfile.replace( "2016-0513-", "" )
    if newstr != myfile:
        print "Renaming %s -> %s." % (myfile, newstr)
        os.rename( myfile, newstr )
