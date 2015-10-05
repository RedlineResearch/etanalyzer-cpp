import sqlite3

def __tup2str__( tup ):
    ret = [ str(x) for x in tup ]
    return tuple(ret)

class Sqorm( object ):
    def __init__( self,
                  tgtpath = None,
                  table = None,
                  keyfield = None ):
        self.conn = sqlite3.connect( tgtpath )
        assert( table != None )
        self.table = str(table)
        self.count = None
        assert( keyfield != None )
        self.keyfield = str(keyfield)

    def __iter__( self ):
        cur = self.conn.cursor()
        cur.execute( "SELECT * FROM %s" % self.table )
        while True:
            reclist = cur.fetchmany()
            if len(reclist) > 0:
                for rec in reclist:
                    yield rec
            else:
                raise StopIteration

    def __contains__( self, item ):
        cur = self.conn.cursor()
        cur.execute( "SELECT * FROM %s WHERE %s=%s" %
                     ( self.table, self.keyfield, str(item) ) )
        retlist = cur.fetchmany()
        if len(retlist) != 1:
            return False
        return True

    def __eq__(self, obj):
        raise NotImplemented

    def __len__(self):
        cur = self.conn.cursor()
        self.count = cur.execute( "SELECT Count(*) FROM %s" % self.table )
        print "%s : %s" %(str(self.count), str(type(self.count)))

    def __additem__(self, key):
        pass

    def __getitem__(self, key):
        cur = self.conn.cursor()
        cur.execute( "SELECT * FROM %s WHERE %s=%s" %
                     ( self.table, self.keyfield, str(key) ) )
        retlist = cur.fetchmany()
        if len(retlist) != 1:
            raise KeyError( "%s not found" % str(key ) )
        return retlist[0]

    def __setitem__(self, key, value):
        cur = self.conn.cursor()
        newval = __tup2str__( value )
        # cmd = "INSERT OR REPLACE INTO %s VALUES %s" % ( self.table, str((key,) + newval) )
        # print "CMD: %s" % cmd
        cur.execute( "INSERT OR REPLACE INTO %s VALUES %s" %
                     ( self.table, str((key,) + newval) ) )

    def __delitem__(self, key):
        raise NotImplemented

    def close( self ):
        if self.conn != None:
            self.conn.commit()
            self.conn.close()

__all__ = [ "Sqorm", ]

if __name__ == "__main__":
    import argparse

    def setup_logger( targetdir = ".",
                      filename = "lifeparse.log",
                      debugflag = False ):
        logger_name = "sqorm"
        # Set up main logger
        dbflag = logging.DEBUG if debugflag else logging.WARNING
        logger = logging.getLogger( logger_name )
        formatter = logging.Formatter( '[%(funcName)s] : %(message)s' )
        logger.setLevel( dbflag  )
        filehandler = logging.FileHandler( os.path.join( targetdir, filename ) , 'w' )
        filehandler.setLevel( dbflag )
        filehandler.setFormatter( formatter )
        logger.addHandler( filehandler )
        return logger

    def intWithCommas(x):
        if type(x) not in [type(0), type(0L)]:
            raise TypeError("Parameter must be an integer.")
        if x < 0:
            return '-' + intWithCommas(-x)
        result = ''
        while x >= 1000:
            x, r = divmod(x, 1000)
            result = ",%03d%s" % (r, result)
        return "%d%s" % (x, result)

    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "target", help = "Target db file in sqlite3 format." )
    parser.add_argument( "table", help = "Table name in db." )
    parser.add_argument( "keyfield", help = "Keyfield for db." )
    args = parser.parse_args()

    sqobj = Sqorm( tgtpath = args.target,
                   table = args.table,
                   keyfield = args.keyfield )
    total = 0
    print "==========================================================================="
    print "First run:"
    for x in sqobj:
        if total == 0:
            savekey = x[0]
        if total % 100000 == 0:
            print str(x)
        total = total + 1
    print "Total objects    : %s" % intWithCommas( total )
    total = 0
    print "==========================================================================="
    print "Second run:"
    for x in sqobj:
        if total % 900000 == 0:
            print str(x)
        total = total + 1
    print "Total objects    : %s" % intWithCommas( total )
    print "==========================================================================="
    print "Contents of key[ %s ]" % str(savekey)
    print "     ", sqobj[savekey]
    print "Attempting to set key[ %s ]" % str(savekey)
    oldval = sqobj[savekey]
    val = (oldval[1], 99, 99, 99, 99)
    sqobj[savekey] = val
    print "==========================================================================="
    print "Checking new contents of key[ %s ] after" % str(savekey)
    print "     ", sqobj[savekey]
    print "==========================================================================="
    print "Attempting to set key[ %s ] to old value" % str(savekey)
    sqobj[savekey] = oldval[1:]
    print "Checking restored contents of key[ %s ] after" % str(savekey)
    print "     ", sqobj[savekey]
    print "==========================================================================="
