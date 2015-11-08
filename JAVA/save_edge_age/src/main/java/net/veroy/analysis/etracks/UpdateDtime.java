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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;


public class UpdateDtime {
    private static Cache<Integer, ObjectRecord> cache;
    private static Connection conn;
    private final static String table = "objects";

    public static void main(String[] args) {
        RemovalListener<Integer, ObjectRecord> remListener = new RemovalListener<Integer, ObjectRecord>() {
              public void onRemoval(RemovalNotification<Integer, ObjectRecord> removal) {
                  // int objId = removal.getKey();
                  ObjectRecord rec = removal.getValue();
                  try {
                      putIntoDB( rec );
                  } catch ( Exception e ) {
                      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                      System.out.println( e.getClass().getName() + ": " + e.getMessage() );
                      System.exit(0);
                  }
              }
        };
        cache = CacheBuilder.newBuilder()
            .maximumSize(10000000)
            .removalListener( remListener )
            .build();
        conn = null;
        Statement stmt = null;
        String dbname = args[0];
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbname);
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
        stmt.executeUpdate( String.format( "INSERT OR REPLACE INTO %s" +
                                           "(objid,objtype,size,length,atime,dtime,allocsite) " +
                                           " VALUES (%d,'%s',%d,%d,%d,%d,%d);",
                                           table, objId, objtype, size, length, atime, dtime, allocsite ) );
        return true;
    }

    private static void processInput() throws SQLException, ExecutionException {
        try {
            int i = 0;
            String line;
            try (
                  InputStreamReader isr = new InputStreamReader(System.in, Charset.forName("UTF-8"));
                  BufferedReader bufreader = new BufferedReader(isr);
            ) {
                int timeByMethod = 0;
                while ((line = bufreader.readLine()) != null) {
                    // Deal with the line
                    String[] fields = line.split(" ");
                    if (isMethod(fields[0])) {
                        // TODO HERE TODO 2015-1105 RLV
                        timeByMethod += 1;
                        // What else to do this?
                    } else if (isDeath(fields[0])) {
                        // TODO HERE TODO 2015-1105 RLV
                        int objId = 0;
                        // Convert objId to integer
                        try {
                            objId = Integer.parseInt(fields[1], 16);
                        } catch( Exception e ) {
                            System.out.println( String.format("parseInt failed: %s", fields[1] ) );
                            i += 1;
                            continue;
                        }
                        // Update record with death time timeByMethod
                        updateDeathTime( objId, timeByMethod );
                    }
                    i += 1;
                    if (i % 10000 == 1) {
                        System.out.print(".");
                    } 
                }
            }
            System.out.println(""); // Add a newline to the progress bar.
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

    private static boolean isMethod( String op ) {
        // Only count method entry and exception handled.
        // TODO: Verify this.
        return (op.equals("M") || op.equals("H"));
    }

    private static boolean isDeath(String op) {
        return op.equals("D");
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
            // return putIntoDB( rec );
        }
        return true;
    }
}
