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

public class SaveObjectInfo {
    private static Connection conn;
    private final static String table = "objects";

    public static void main(String[] args) {
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

    private static boolean createDB() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate( String.format( "DROP TABLE IF EXISTS %s", table ) );
        stmt.executeUpdate( String.format( "CREATE TABLE %s " +
                                           "( objId INTEGER PRIMARY KEY, objtype TEXT," +
                                           "  size INTEGER, length INTEGER," +
                                           "  atime INTEGER, dtime INTEGER," +
                                           "  allocsite INTEGER )",
                                           table ) );
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
                    if (isAllocation(fields[0])) {
                        ObjectRecord object = parseAllocation( fields, timeByMethod );
                        putIntoDB( object );
                    }
                    i += 1;
                    if (i % 10000 == 1) {
                        System.out.print(".");
                    } 
                }
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

    private static ObjectRecord parseAllocation( String[] fields, int timeByMethod ) {
        // System.out.println("[" + fields[0] + "]");
        int objId = Integer.parseInt( fields[1], 16 );
        String objtype = fields[3];
        int size = Integer.parseInt( fields[2], 16 );
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
}
