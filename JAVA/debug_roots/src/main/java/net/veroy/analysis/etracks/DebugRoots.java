package net.veroy.analysis.etracks;


import net.veroy.analysis.etracks.EdgeRecord;
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
import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.HashMultimap;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.javatuples.Quartet;

public class DebugRoots {
    private static HashMap<Integer, Pair<Integer, Integer>> edge_map;
    // edge_map: objId -> (srcId, fieldId)
    private static Connection conn;
    private final static String edge_table = "edges";
    private final static String root_table = "roots";
    private final static String metadata_table = "metadata";
    private static int timeByMethod = 0;
    private static boolean doneFlag = false;
    private static int index_g = 0;

    public static void main(String[] args) {
        edge_map = new HashMap();
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

    private static boolean putIntoDB( EdgeRecord newrec ) throws SQLException {
        Statement stmt = conn.createStatement();
        int tgtId = newrec.get_tgtId();
        int srcId = newrec.get_srcId();
        int fieldId = newrec.get_fieldId();
        int dtime = newrec.get_dtime();
        if ((dtime == 0) && (doneFlag)) {
            // If we haven't set the death time, and the trace is done,
            // use the current timeByMethod as the dtime.
            dtime = timeByMethod;
        }
        stmt.executeUpdate( String.format( "INSERT OR REPLACE INTO %s" +
                                           "(tgtId,srcId,fieldId,dtime) " +
                                           " VALUES (%d,%d,%d,%d);",
                                           edge_table, tgtId, srcId, fieldId, dtime ) );
        return true;
    }

    private static boolean createDB() throws SQLException {
        Statement stmt = conn.createStatement();
        // Drop tables.
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", edge_table ) );
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", root_table ) );
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", metadata_table ) );
        // Create tables.
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( tgtId INTEGER," +
                                           "  srcId INTEGER, fieldId INTEGER," +
                                           "  dtime INTEGER )",
                                           edge_table ) );
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( tgtId INTEGER, rtime INTEGER )",
                                           root_table ) );
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( finaltime INTEGER )",
                                           metadata_table ) );
        return true;
    }

    private static boolean putIntoEdgeMap( Integer tgtId,
                                           Integer srcId,
                                           Integer fieldId ) {
        edge_map.put( tgtId, Pair.with( srcId, fieldId ) );
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
                    if (isMethod( fields[0])) {
                        timeByMethod += 1;
                    } else if (isRoot(fields[0])) {
                        // TODO 
                    } else if (isDeath(fields[0])) {
                        int objId = parseDeath( fields );
                    } else if (isUpdate(fields[0])) {
                        // U <old-target-id> <object-id> <new-target-id> <field-id> <thread-id>
                        UpdateRecord update = parseUpdate( fields, timeByMethod );
                        int objId = update.get_objId();
                        int oldTgtId = update.get_oldTgtId();
                        int newTgtId = update.get_newTgtId();
                        int fieldId = update.get_fieldId();
                        if (newTgtId > 0) {
                            // TODO Do we need alloc time? Or death time?
                            putIntoEdgeMap( newTgtId,
                                            objId,
                                            fieldId );
                            if (index_g % 10000 == 1) {
                                System.out.print(".");
                            } 
                        }
                    }
                    index_g += 1;
                }
                doneFlag = true;
                System.out.print("\nInvalidating cache:");
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

    private static boolean isUpdate( String op ) {
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

    private static boolean isRoot( String op ) {
        return op.equals("R");
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

    private static Integer parseDeath( String[] fields ) {
        return Integer.parseInt( fields[1], 16 );
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

}
