package net.veroy.analysis.etracks;


import net.veroy.analysis.etracks.ObjectRecord;
import net.veroy.analysis.etracks.UpdateRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.HashMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class SaveObjectInfo {
    private static Cache<Integer, ObjectRecord> cache;
    private static Connection conn;
    private final static String table = "objects";
    private final static String metadata_table = "metadata";
    private static int timeByMethod = 0;
    private static HashMap<Integer, Boolean> dtimeMap = new HashMap();
    private static boolean doneFlag = false;
    private static int index_g = 0;

    public static void main(String[] args) {
        RemovalListener<Integer, ObjectRecord> remListener = new RemovalListener<Integer, ObjectRecord>() {
              public void onRemoval(RemovalNotification<Integer, ObjectRecord> removal) {
                  // int objId = removal.getKey();
                  ObjectRecord rec = removal.getValue();
                  try {
                      putIntoDB( rec );
                        index_g += 1;
                        if (index_g % 10000 == 1) {
                            System.out.print(">");
                        } 
                  } catch ( Exception e ) {
                      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                      System.out.println( e.getClass().getName() + ": " + e.getMessage() );
                      System.exit(0);
                  }
              }
        };
        cache = CacheBuilder.newBuilder()
            .maximumSize(250000000) 
            .removalListener( remListener )
            .build(); // TODO Make maximumSize a command line arg with default TODO TODO
        conn = null;
        Statement stmt = null;
        String dbname = args[0];
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbname);
            createDB();
            processInput();
            conn.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.out.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        System.out.println("Database ran successfully");
    }

    private static ObjectRecord getFromDB( int objId ) throws SQLException {
        ObjectRecord objrec = new ObjectRecord();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery( String.format("SELECT * FROM %s WHERE objid=%d;",table ,objId ) );
        if (rs.next()) {
            objrec.set_objId( rs.getInt("objid") );
            objrec.set_size( rs.getInt("size") );
            objrec.set_objtype( rs.getString("objtype") );
            objrec.set_length( rs.getInt("length") );
            objrec.set_atime( rs.getInt("atime") );
            objrec.set_dtime( rs.getInt("dtime") );
            objrec.set_allocsite( rs.getInt("allocsite") );
        } else {
            objrec.set_objId( objId );
        }
        return objrec;
    }

    private static boolean putIntoDB( ObjectRecord newrec ) throws SQLException {
        Statement stmt = conn.createStatement();
        int objId = newrec.get_objId();
        int size = newrec.get_size();
        int length = newrec.get_length();
        int atime = newrec.get_atime();
        int dtime = newrec.get_dtime();
        int allocsite = newrec.get_allocsite();
        String objtype = newrec.get_objtype();
        if ((dtime == 0) && (doneFlag)) {
            dtime = timeByMethod;
            dtimeMap.put(objId, true);
        }
        stmt.executeUpdate( String.format( "INSERT OR REPLACE INTO %s" +
                                           "(objid,objtype,size,length,atime,dtime,allocsite) " +
                                           " VALUES (%d,'%s',%d,%d,%d,%d,%d);",
                                           table, objId, objtype, size, length, atime, dtime, allocsite ) );
        return true;
    }

    private static boolean createDB() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", table ) );
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( objId INTEGER PRIMARY KEY, objtype TEXT," +
                                           "  size INTEGER, length INTEGER," +
                                           "  atime INTEGER, dtime INTEGER," +
                                           "  allocsite INTEGER )",
                                           table ) );
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", metadata_table ) );
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( finaltime INTEGER )",
                                           metadata_table ) );
        return true;
    }

    private static void processInput() throws SQLException, ExecutionException {
        try {
            String line;
            try (
                  InputStreamReader isr = new InputStreamReader(System.in, Charset.forName("UTF-8"));
                  BufferedReader bufreader = new BufferedReader(isr);
            ) {
                while ((line = bufreader.readLine()) != null) {
                    // Deal with the line
                    String[] fields = line.split(" ");
                    if (isAllocation(fields[0])) {
                        ObjectRecord object = parseAllocation( fields, timeByMethod );
                        // putIntoDB( object );
                        int objId = object.get_objId();
                        cache.put(objId, object);
                        dtimeMap.put(objId, false);
                    } else if (isMethod( fields[0])) {
                        timeByMethod += 1;
                    } else if (isDeath(fields[0])) {
                        // TODO HERE TODO 2015-1105 RLV
                        int objId = 0;
                        // Convert objId to integer
                        try {
                            objId = Integer.parseInt(fields[1], 16);
                        } catch( Exception e ) {
                            System.out.println( String.format("parseInt failed: %s", fields[1] ) );
                            index_g += 1;
                            continue;
                        }
                        // Update record with death time timeByMethod
                        updateDeathTime( objId, timeByMethod );
                    }
                    index_g += 1;
                    if (index_g % 10000 == 1) {
                        System.out.print(".");
                    } 
                }
                doneFlag = true;
                System.out.print("\nInvalidating cache:");
                cache.invalidateAll();
                Statement stmt = conn.createStatement();
                stmt.executeUpdate( String.format( "INSERT OR REPLACE INTO %s" +
                                                   "(finaltime) VALUES (%d);",
                                                   metadata_table, timeByMethod ) );
            }
            System.out.println("");
        } catch (IOException e) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
    }

    private static boolean isUpdate(String op) {
        return op.equals("U");
    }

    private static boolean isAllocation( String op ) {
        return (op.equals("A") || op.equals("N") || op.equals("P") || op.equals("I") || op.equals("V"));
    }

    private static boolean isDeath(String op) {
        return op.equals("D");
    }

    private static boolean isMethod( String op ) {
        // Only count method entry and exception handled.
        // TODO: Verify this.
        return (op.equals("M") || op.equals("H"));
    }

    private static ObjectRecord parseAllocation( String[] fields, int timeByMethod ) {
        // System.out.println("[" + fields[0] + "]");
        int objId = Integer.parseInt( fields[1], 16 );
        int size = Integer.parseInt( fields[2], 16 );
        String objtype = fields[3];
        int allocsite = Integer.parseInt( fields[4], 16 );
        int length = Integer.parseInt( fields[5], 16 );
        return new ObjectRecord( objId,
                                 objtype,
                                 size,
                                 length,
                                 timeByMethod, // alloctime
                                 0, // death - Unknown at this point TODO
                                 allocsite );
    }

    private static UpdateRecord parseUpdate( String[] fields, int timeByMethod ) {
        int oldTgtId = Integer.parseInt( fields[1], 16 );
        int objId = Integer.parseInt( fields[2], 16 );
        int newTgtId = Integer.parseInt( fields[3], 16 );
        int fieldId = 0;
        try {
            fieldId = Integer.parseInt( fields[4], 16 );
        }
        catch ( Exception e ) {
            try {
                System.out.println( String.format("parseInt failed: %d -> %s", objId, fields[4]) );
                BigInteger tmp = new BigInteger( fields[4], 16 );
                fieldId = tmp.intValue();
            }
            catch ( Exception e2 ) {
                System.err.println( e2.getClass().getName() + ": " + e2.getMessage() );
                System.exit(0);
            }
        }
        int threadId = Integer.parseInt( fields[5], 16 );
        return new UpdateRecord( objId,
                                 oldTgtId,
                                 newTgtId,
                                 fieldId,
                                 threadId,
                                 timeByMethod );
    }

    private static boolean updateDeathTime( int objId, int timeByMethod ) throws SQLException {
        // Get record first then fill in death time
        ObjectRecord rec;
        try {
            // rec = getFromDB(objId);
            rec = cache.get( objId,
                    new Callable<ObjectRecord>() {
                        public ObjectRecord call() throws SQLException {
                            return getFromDB( objId );
                        }
                    } );
        } catch( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.out.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
            return false;
        }
        int old_dtime = rec.get_dtime();
        // System.out.println(  old_dtime + " -> " + timeByMethod );
        if (old_dtime != timeByMethod) {
            rec.set_dtime( timeByMethod );
            cache.put( objId, rec );
        }
        dtimeMap.put( objId, true );
        return true;
    }
}
