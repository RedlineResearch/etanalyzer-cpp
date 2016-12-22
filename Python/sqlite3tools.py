import sqlite3
import os
import itertools

__all__ = [ "show_tables", "show_all_records", "get_row_count",
            "get_all_rows", "get_tables" ]
tables_cmd = "SELECT name FROM sqlite_master\n\t\t WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'\n\t\t UNION ALL\n\t\t SELECT name FROM sqlite_temp_master\n\t\t WHERE type IN ('table','view')\n\t\t ORDER BY 1"

def get_tables( cursor ):
    global tables_cmd
    cursor.execute( tables_cmd )
    return [ x for x in cursor ]

def show_tables( cursor ):
    total = 0
    for row in get_tables( cursor ):
        print row
        total += 1
    print "Total tables : ", total

def show_all_records( cursor, table ):
    cursor.execute( 'SELECT * FROM ' + table )
    total = 0
    for row in cursor:
        print row
        total += 1
    print "Total rows : ", total

def get_row_count( cursor, table ):
    # The execute returns a cursor with a single row.
    # fetchone() returns a tuple with a single entry, selected with [0]
    count = cursor.execute( 'SELECT Count(*) FROM ' + table ).fetchone()[0]
    return count

# TODO: Not sanitizing the SQL but we're all friends here!
def get_select_row_count( cursor, table, sqlcmd ):
    # The execute returns a cursor with a single row.
    # fetchone() returns a tuple with a single entry, selected with [0]
    count = cursor.execute( 'SELECT Count(*) FROM ' + table +
                            ' WHERE ' + sqlcmd ).fetchone()[0]
    return count

def get_all_rows( cursor, table ):
    """Get all rows from table. Returns a list."""
    mylist = []
    cursor.execute( 'SELECT * FROM ' + table )
    for row in cursor:
        mylist.append( row )
    return mylist
