import os
import sys
import logging
from optparse import OptionParser, OptionGroup
import pprint

# Exported names
heap_alloc_list = [ "A", "I", "N", "P", "V", ]
heap_op_list = heap_alloc_list + [ "D", "U", ]
meth_op_list = [ "M", "E", # method entry and exit
                 "T", "H", "X", # exception related
                 ]
valid_op_list = heap_op_list + meth_op_list + [ "R", ]
                                 

pp = pprint.PrettyPrinter( indent = 4 )

# TODO TODO TODO
heap_entry_fields = [ 'fields',
                      'objId',
                      'rectype',
                      'size',
                      'threadId',
                      'type',
                      'methodId',
                      'fieldId' ]
# TODO TODO TODO

#
# Main processing
#
def is_valid_op( op = None ):
    global valid_op_list
    return op in valid_op_list
        
def is_heap_op( op = None ):
    global heap_op_list
    return op in heap_op_list
        
def is_heap_alloc_op( op = None ):
    global heap_alloc_list
    return op in heap_alloc_list
        
def is_method_op( op = None ):
    global meth_op_list
    return op in meth_op_list

def parse_line( line = None,
                hex2decflag = False,
                logger = None ):
    # Return a dictionary with keys:
    #    rectype - ALL
    #    objId - A,D,U,M,E,R,X,T,H
    #    threadId - A,U,M,E,R,X,T,H
    #    type - A
    #    size - A
    #    newTgtId - U
    #    oldTgtId - U
    assert( line != None )
    return __parse_line_nocheck( line = line,
                                 hex2decflag = hex2decflag,
                                 logger = logger )

def hex2dec( val ):
    try:
        retval = int(val, 16)
    except:
        return None
    return retval
    
def __parse_line_nocheck( line = None,
                          hex2decflag = False,
                          logger = None ):
    # Return a dictionary with keys:
    # ====================================
    #    rectype - ALL
    #    objId - {A,I,N,P,V},D,U,M,E,T,H,X,R
    #    threadId - {A,I,N,P,V},U,M,E,T,H,X,R
    #    type - {A,I,N,P,V}
    #    size - {A,I,N,P,V}
    #    newTgtId - U
    #    oldTgtId - U
    #    fieldId - U
    #    methodId - M,E,T,H
    #    exceptionId - T,H
    ret = {}
    a = line.split()
    # Get object Id
    if ( a[0] in heap_alloc_list or
         a[0] == "D" or a[0] == "R" ):
        objId = hex2dec(a[1])
    elif ( a[0] == "H" ):
        objId = hex2dec(a[3])
    else: # everything else
        objId = hex2dec(a[2])
    if (objId == None) or (type(objId) != type(1)):
        print "ERROR DEBUG:"
        print "line: %s" % str(line)
        print "objId: %s" % str(objId)
        exit(2000)
    if a[0] in heap_alloc_list:
        ret["rectype"] = a[0]
        ret["objId"] = objId
        ret["size"] = hex2dec(a[2])
        ret["type"] = a[3]
        ret["site"] = hex2dec(a[4])
        ret["length"] = hex2dec(a[5])
        ret["threadId"] = hex2dec(a[6])
    elif a[0] == "D":
        ret["rectype"] = "D"
        ret["objId"] = objId
    elif a[0] == "U":
        ret["rectype"] = "U"
        ret["oldTgtId"] = hex2dec(a[1])
        ret["objId"] = objId
        ret["newTgtId"] = hex2dec(a[3])
        ret["fieldId"] = hex2dec(a[4])
        ret["threadId"] = hex2dec(a[5])
    elif a[0] == "M":
        ret["rectype"] = "M"
        ret["methodId"] = a[1]
        ret["objId"] = objId
        ret["threadId"] = hex2dec(a[3])
    elif a[0] == "E":
        ret["rectype"] = a[0]
        ret["methodId"] = a[1]
        ret["objId"] = objId
        ret["threadId"] = hex2dec(a[3])
    elif a[0] == "T" or a[0] == "H" or a[0] == "X":
        ret["rectype"] = a[0]
        ret["methodId"] = a[1]
        ret["objId"] = objId
        ret["exceptionId"] = a[3]
        ret["threadId"] = hex2dec(a[4])
    elif a[0] == "R":
        ret["rectype"] = a[0]
        ret["objId"] = objId
        ret["threadId"] = hex2dec(a[2])
    else:
        print "Unknown record type: %s" % a[0]
        print "-------------------------------------"
        pp.pprint( a )
        raise ValueError( "Unknown record type: %s" % a[0] )
    assert( ret != None )
    return ret

__all__ = [ heap_entry_fields, is_valid_op, is_heap_alloc_op, is_heap_op, parse_line, \
            is_method_op, ]

if __name__ == "__main__":
    # TODO
    pass
