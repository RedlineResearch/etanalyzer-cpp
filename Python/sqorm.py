import sqlite3
import collections
import pylru

def __tup2str__( tup ):
    ret = [ str(x) for x in tup ]
    return tuple(ret)

class ObjectCache( collections.Mapping ):
    def __init__( self,
                  tgtpath = None,
                  table = None,
                  keyfield = None,
                  cachesize = 5000000,
                  logger = None ):
        self.conn = sqlite3.connect( tgtpath )
        assert( table != None )
        self.table = str(table)
        self.count = None
        assert( keyfield != None )
        self.keyfield = str(keyfield)
        self.lru = pylru.lrucache( size = cachesize )
        # NOTE: We assume that the keyfield is always the first field in the record
        #       tuple.
        self.logger = logger

    def __iter__( self ):
        cur = self.conn.cursor()
        cur.execute( "SELECT * FROM %s" % self.table )
        while True:
            reclist = cur.fetchmany()
            if len(reclist) > 0:
                for rec in reclist:
                    key = rec[0]
                    if key not in self.lru:
                        self.lru[key] = rec[1:]
                    yield key
            else:
                raise StopIteration

    def iteritems( self ):
        cur = self.conn.cursor()
        cur.execute( "SELECT * FROM %s" % self.table )
        while True:
            reclist = cur.fetchmany()
            if len(reclist) > 0:
                for rec in reclist:
                    key = rec[0]
                    if key not in self.lru:
                        self.lru[key] = rec[1:]
                    yield (key, rec[1:])
            else:
                raise StopIteration

    def keys( self ):
        cur = self.conn.cursor()
        cur.execute( "SELECT %s FROM %s" % (self.keyfield, self.table) )
        keyset = set()
        while True:
            keylist = cur.fetchmany()
            if len(keylist) > 0:
                keyset.update( [ x[0] for x in keylist ] )
            else:
                break
        result = list(keyset)
        for x in result:
            try:
                assert(type(x) == type(1))
            except:
                print "kEY ERROR:"
                print "x:", x
                exit(100)
        return result

    def __contains__( self, item ):
        if item in self.lru:
            return True
        else:
            cur = self.conn.cursor()
            cmd = "SELECT * FROM %s WHERE %s=%s" % ( self.table, self.keyfield, str(item) )
            self.logger.debug( "CMD: %s" % cmd )
            cur.execute( cmd )
            retlist = cur.fetchmany()
            if len(retlist) != 1:
                return False
            rec = retlist[0]
            self.lru[item] = rec[1:]
            return True

    def __len__(self):
        cur = self.conn.cursor()
        if self.count == None:
            cur.execute( "SELECT Count(*) FROM %s" % self.table )
        self.count = cur.fetchone()
        # DEBUG: print "%s : %s" %(str(self.count), str(type(self.count)))
        return self.count[0]

    def __getitem__(self, key):
        if key in self.lru:
            return self.lru[key]
        else:
            cur = self.conn.cursor()
            cur.execute( "select * from %s where %s=%s" %
                         ( self.table, self.keyfield, str(key) ) )
            retlist = cur.fetchmany()
            if len(retlist) < 1:
                raise KeyError( "%s not found" % str(key ) )
            elif len(retlist) > 1:
                pass
                # todo: need to log an error here. or at least a warning.
            rec = retlist[0]
            key = rec[0]
            self.lru[key] = rec[1:]
            return rec[1:]

    def getitem_from_table(self, key, mytable, mykeyfield):
        cur = self.conn.cursor()
        cur.execute( "select * from %s where %s=%s" %
                     ( mytable, mykeyfield, str(key) ) )
        retlist = cur.fetchmany()
        if len(retlist) < 1:
            raise KeyError( "%s not found" % str(key ) )
        elif len(retlist) > 1:
            pass
            # todo: need to log an error here. or at least a warning.
        rec = retlist[0]
        return rec

    def close( self ):
        if self.conn != None:
            self.conn.close()

    def __additem__(self, key):
        raise NotImplemented

    def __setitem__(self, key, value):
        raise NotImplemented

    def __delitem__(self, key):
        raise NotImplemented

__all__ = [ "ObjectCache", ]

if __name__ == "__main__":
    import argparse

    def setup_logger( targetdir = ".",
                      filename = "lifeparse.log",
                      debugflag = False ):
        logger_name = "objectcache"
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

    sqobj = ObjectCache( tgtpath = args.target,
                   table = args.table,
                   keyfield = args.keyfield )
    raise RuntimeError("TODO")
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
