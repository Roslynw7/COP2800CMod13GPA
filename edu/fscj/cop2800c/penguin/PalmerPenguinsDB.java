// PenguinRookeryDB.java
// R.Williams
// 4/23/2026
// Class for Penguin Rookery DB operations

package edu.fscj.cop2800c.penguin;

import java.sql.*;
import java.util.ArrayList;

public class PalmerPenguinsDB {
    public static void createDB(ArrayList<Penguin> penguins) {
        final String DB_NAME = "PalmerPenguins";
        final String CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        final String CONN_URL = "jdbc:sqlserver://localhost:1433;integratedSecurity=true;";
        final String SQL_DROP_TABLE = "DROP TABLE IF EXISTS Penguin";  // More robust drop query

        try {
            Class.forName(CLASS_NAME);

            try (Connection con = DriverManager.getConnection(CONN_URL);
                 Statement stmt = con.createStatement()) {

                try {
                    stmt.executeUpdate("CREATE DATABASE " + DB_NAME);
                    System.out.println("DB created");
                } catch (SQLException e) {
                    System.out.println("Could not create DB, it might already exist.");
                }

                // Switch to the new DB
                stmt.executeUpdate("USE " + DB_NAME);

                // Create table
                String createTable = "CREATE TABLE Penguin " +
                        "(SAMPLENUM smallint PRIMARY KEY NOT NULL," +
                        "CULMENLEN float NOT NULL," +
                        "CULMENDEPTH float NOT NULL," +
                        "BODYMASS smallint NOT NULL," +
                        "SEX char(1) NOT NULL," +
                        "SPECIES varchar(20) NOT NULL," +
                        "FLIPPERLEN float NOT NULL)";
                stmt.executeUpdate(createTable);
                System.out.println("Table created");

                // Insert records using batch with try-with-resources
                String insertQuery = "INSERT INTO Penguin (SAMPLENUM, CULMENLEN, CULMENDEPTH, " +
                        "BODYMASS, SEX, SPECIES, FLIPPERLEN) VALUES (?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement pstmt = con.prepareStatement(insertQuery)) {
                    for (Penguin penguin : penguins) {
                        pstmt.setInt(1, penguin.getSampleNum());
                        pstmt.setDouble(2, penguin.getCulmenLength());
                        pstmt.setDouble(3, penguin.getCulmenDepth());
                        pstmt.setInt(4, (int) penguin.getBodyMass());

                        // Set SEX with null-check and a fallback value
                        String sex = penguin.getSex();
                        pstmt.setString(5, (sex != null && !sex.isEmpty()) ? sex.substring(0, 1) : "U");  // Default to "U" for Unknown
    
                        pstmt.setString(6, penguin.getSpecies().toString());
                        pstmt.setDouble(7, penguin.getFlipperLength());

                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                    System.out.println("Data inserted");
                }

                // Query and print results using try-with-resources
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM Penguin")) {
                    while (rs.next()) {
                        System.out.println(
                            rs.getInt("SAMPLENUM") + "," +
                            rs.getDouble("CULMENLEN") + "," +
                            rs.getDouble("CULMENDEPTH") + "," +
                            rs.getInt("BODYMASS") + "," +
                            rs.getString("SEX") + "," +
                            rs.getString("SPECIES") + "," +
                            rs.getDouble("FLIPPERLEN")
                        );
                    }
                } catch (SQLException e) {
                    System.out.println("Error reading data from the table.");
                    e.printStackTrace();
                }

                // Drop the table
                stmt.executeUpdate(SQL_DROP_TABLE);
                System.out.println("Penguin table dropped");

                
                try {
                    stmt.executeUpdate("DROP DATABASE IF EXISTS " + DB_NAME);
                    System.out.println("DB dropped");
                } catch (SQLException e) {
                    System.out.println("Could not drop DB, it might be in use.");
                }

            } catch (SQLException e) {
                System.out.println("Error with database connection or statement.");
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            System.out.println("SQL Server JDBC driver not found.");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
