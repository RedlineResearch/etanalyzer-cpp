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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.javatuples.Quartet;

public class SaveEdgeInfo {
    //TODO DeleteMe:  private static Cache<Integer, EdgeRecord> cache2;
    private static Cache<Quartet<Integer, Integer, Integer, Integer>, Integer> cache;
    private static HashMap<Triplet, Integer> edge_map;
    private static HashMap<Integer,
                           HashMap<Integer,
                                   HashSet<Pair<Integer, Integer>>>> objref_map;
    // objId ->
    //     fieldId -> HashSet of pairs of (target Id, allocation time)
    // Note: this would be a LOT easier if Java allowed typedefs.
    // I'm sorely tempted to use the class extension as typedef technique
    // which is widely panned. - RLV 2015-1219
    private static Connection conn;
    private final static String table = "edges";
    private final static String metadata_table = "metadata";
    private static int timeByMethod = 0;
    private static boolean doneFlag = false;
    private static int index_g = 0;

    public static void main(String[] args) {
        RemovalListener<Quartet<Integer, Integer, Integer, Integer>, Integer> remListener = 
            new RemovalListener<Quartet<Integer, Integer, Integer, Integer>, Integer>() {
              public void onRemoval( RemovalNotification<Quartet<Integer, Integer, Integer, Integer>,
                                     Integer> removal ) {
                  Quartet<Integer, Integer, Integer, Integer> tuple = removal.getKey();
                  Integer dtime = removal.getValue();
                  EdgeRecord rec = new EdgeRecord( tuple.getValue0(),
                                                   tuple.getValue1(),
                                                   tuple.getValue2(),
                                                   tuple.getValue3(),
                                                   dtime );

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
        edge_map = new HashMap();
        // objref_map = HashMap<Integer, HashMap<Integer, HashSet<Pair<Integer, Integer>>>>();
        objref_map = new HashMap<Integer, HashMap<Integer, HashSet<Pair<Integer, Integer>>>>();
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

    private static EdgeRecord getFromDB( int tgtId ) throws SQLException {
        EdgeRecord edgerec = new EdgeRecord();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery( String.format("SELECT * FROM %s WHERE tgtId=%d;",table ,tgtId ) );
        if (rs.next()) {
            edgerec.set_tgtId( rs.getInt("tgtId") );
            edgerec.set_srcId( rs.getInt("srcId") );
            edgerec.set_fieldId( rs.getInt("fieldId") );
            edgerec.set_atime( rs.getInt("atime") );
            edgerec.set_dtime( rs.getInt("dtime") );
        } else {
            edgerec.set_tgtId( tgtId );
        }
        return edgerec;
    }

    private static boolean putIntoDB( EdgeRecord newrec ) throws SQLException {
        Statement stmt = conn.createStatement();
        int tgtId = newrec.get_tgtId();
        int srcId = newrec.get_srcId();
        int fieldId = newrec.get_fieldId();
        int atime = newrec.get_atime();
        int dtime = newrec.get_dtime();
        if ((dtime == 0) && (doneFlag)) {
            // If we haven't set the death time, and the trace is done,
            // use the current timeByMethod as the dtime.
            dtime = timeByMethod;
        }
        stmt.executeUpdate( String.format( "INSERT OR REPLACE INTO %s" +
                                           "(tgtId,srcId,fieldId,atime,dtime) " +
                                           " VALUES (%d,%d,%d,%d,%d);",
                                           table, tgtId, srcId, fieldId, atime, dtime ) );
        return true;
    }

    private static boolean createDB() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", table ) );
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( tgtId INTEGER," +
                                           "  srcId INTEGER, fieldId INTEGER," +
                                           "  atime INTEGER, dtime INTEGER )",
                                           table ) );
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", metadata_table ) );
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( finaltime INTEGER, numberEdges INTEGER )",
                                           metadata_table ) );
        return true;
    }

    // Triplet is <srcId, tgtId, fieldId>
    private static boolean putIntoObjrefMap(Triplet<Integer, Integer, Integer> tuple, int atime) {
        Integer srcId = tuple.getValue0();
        Integer tgtId = tuple.getValue1();
        Integer fieldId = tuple.getValue2();
        // Check to see srcId is in objref_map
        if (!objref_map.containsKey( srcId )) {
            HashMap< Integer, HashSet<Pair<Integer, Integer>>> new_field_map =
                new HashMap< Integer, HashSet<Pair<Integer, Integer>>>();
            objref_map.put( srcId, new_field_map );
        }
        // Check to see if fieldId is already there
        HashMap< Integer, HashSet<Pair<Integer, Integer>>> field_map =
            objref_map.get( srcId );
        if (!field_map.containsKey( fieldId )) {
            field_map.put( fieldId, new HashSet<Pair<Integer, Integer>>() );
        }
        // Add edge_tuple to objref_map's field_map
        HashSet<Pair<Integer, Integer>> edge_set = field_map.get( fieldId );
        edge_set.add( Pair.with(tgtId, atime) );
        return true;
    }

    private static void saveDeadEdge( Integer srcId, Integer tgtId, Integer fieldId ) {
        if (index_g % 10000 == 1) {
            System.out.print("X");
        } 
        // Save newly dead edge
        Triplet<Integer, Integer, Integer> old_tuple = Triplet.with( srcId, tgtId, fieldId );
        // Get the saved allocation time from edge_map. If that doesn't work, assume the edge
        // was there from the beginning of time (1).
        Integer old_atime = ( edge_map.containsKey(old_tuple) ) ? edge_map.get( old_tuple ) : 1;
        Quartet<Integer, Integer, Integer, Integer> cache_tuple = Quartet.with( srcId,
                                                                                tgtId, 
                                                                                fieldId,
                                                                                old_atime );
        cache.put( cache_tuple, timeByMethod );
    }

    private static int markAllEdgesDead( int objId ) {
        int result = 0; // This fn returns the number of edges marked dead
        // Go through each fieldId
        // private static HashMap<Integer,
        //                        HashMap<Integer,
        //                                HashSet<Pair<Integer, Integer>>>> objref_map;
        // objId ->
        //     fieldId -> HashSet of pairs * see Pair above
        //  
        //  Look for objId in objref_map:
        if (objref_map.containsKey( objId )) {
            // For each target object, add a dead edge.
            HashMap<Integer, HashSet<Pair<Integer, Integer>>> field_map = objref_map.get( objId );
            Iterator it = field_map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry mpair = (Map.Entry) it.next();
                Integer fieldId = (Integer) mpair.getKey();
                HashSet<Pair<Integer, Integer>> tgtset = 
                    (HashSet<Pair<Integer, Integer>>) mpair.getValue();
                // Iterate over tgtset to get tgtId
                Iterator tgt_it = tgtset.iterator();
                while (tgt_it.hasNext()) {
                    Pair<Integer, Integer> tgt_mpair = (Pair<Integer, Integer>) tgt_it.next();
                    // tgtId comes from tgtset.
                    // fieldId is in outer loop.
                    Integer tgtId = (Integer) tgt_mpair.getValue0();
                    Integer atime = (Integer) tgt_mpair.getValue1();
                    saveDeadEdge( objId, tgtId, fieldId );
                }
            }
            return result;
        } else {
            // TODO TODO TODO TODO TODO TODO
            // Log an error?
            // objId SHOULD be in objref_map
        }
        return result;
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
                    // if (isAllocation(fields[0])) {
                    // TODO: Ignore allocations?
                    if (isMethod( fields[0])) {
                        timeByMethod += 1;
                    } else if (isDeath(fields[0])) {
                        int objId = parseDeath( fields );
                        assert( objId > 0 );
                        // Save dead edge
                        markAllEdgesDead( objId );
                    } else if (isUpdate(fields[0])) {
                        // U <old-target-id> <object-id> <new-target-id> <field-id> <thread-id>
                        UpdateRecord update = parseUpdate( fields, timeByMethod );
                        int objId = update.get_objId();
                        int oldTgtId = update.get_oldTgtId();
                        int newTgtId = update.get_newTgtId();
                        int fieldId = update.get_fieldId();
                        Triplet<Integer, Integer, Integer> tuple = Triplet.with( objId, newTgtId, fieldId );
                        if (newTgtId > 0) {
                            // Put live edge into edge_map and objref_map
                            edge_map.put( tuple, timeByMethod );
                            putIntoObjrefMap( tuple, timeByMethod );
                            if (index_g % 10000 == 1) {
                                System.out.print(".");
                            } 
                        }
                        if (oldTgtId > 0) {
                            // Save dead edge
                            saveDeadEdge( objId, oldTgtId, fieldId );
                        }
                    }
                    index_g += 1;
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

    private static boolean isUpdate( String op ) {
        return op.equals("U");
    }

    // TODO: Don't need this? RLV - 2015-1211
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
