package com.test.creditSuisse;

import java.sql.*;

public class DriverManagerConnection {

    Connection con = null;
    Statement stmt = null;
    String db = "jdbc:hsqldb:file:testdb/testdb";
    String user = "SA";
    String password = "";

    public void initialize(String tableName) {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            con = DriverManager.getConnection(db, user, password);
            stmt = con.createStatement();
            checkTableExists(tableName);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() {
        try {
            stmt.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will create entry in database in LOGDETAILS table
     *
     * @param eventId
     * @param eventDuration
     * @param type
     * @param host
     * @param alert
     */
    public void executeInsert(String eventId, Long eventDuration, String type, String host, boolean alert) {
        System.out.println("Inserting records into the table...");
        String sql = "INSERT INTO LOGDETAILS(EventId,EventDuration,Type,Host,Alert) VALUES ('" + eventId + "','" + eventDuration + "ms','" + type + "','" + host + "','" + alert + "')";
        System.out.println(sql);
        int result = 0;
        try {
            result = stmt.executeUpdate(sql);
            if (result > 0) {
                System.out.println("Record is successfully inserted");
            } else {
                System.out.println("Unable to insert record");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will create LOGDETAILS Table in database
     */
    public void createLogTable() {
        String sql = "create table LOGDETAILS(EventId VARCHAR(50),EventDuration VARCHAR(50),Type VARCHAR(50),Host VARCHAR(50),Alert boolean)";
        try {
            int result = stmt.executeUpdate(sql);
            if (result > 0) {
                System.out.println("Record is successfully inserted");
            } else {
                System.out.println("Unable to insert record");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will check if table is present or not, if not it will create new table and insert data in same.
     * @param tableName
     */
    public void checkTableExists(String tableName){
        try {
        DatabaseMetaData dbm = con.getMetaData();
        // check if table is there
        ResultSet tables = dbm.getTables(null, null, tableName, null);
            if (tables.next()) {
                System.out.println(tableName+" table is already present. Inserting data in same.");
            }
            else {
                System.out.println(tableName+" table is not present. Creating a new table");
                createLogTable();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
