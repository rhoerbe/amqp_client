package com.identinetics.edushare.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;


/**
 * Minimal Java application reading from a database and sending them as JSON messages via AQMP.
 */
public class Schule {
    private Connection conn = null;
    private Statement s;
    private ArrayList<Statement> statements = new ArrayList<Statement>(); // list of Statements, PreparedStatements


    void setupDatabase(String[] args) {
        System.out.println("setting up embedded database and data");
        PreparedStatement psInsert;
        try {
            Properties props = new Properties(); // connection properties
            props.put("user", "user1");
            props.put("password", "user1");
            conn = DriverManager.getConnection("jdbc:derby:derbyDB;create=true", props);
            System.out.println("Connected to database");
            s = conn.createStatement();
            statements.add(s);
            try {
                s.execute("DROP TABLE schule");
            } catch (SQLException e) {
                ;
            }
            s.execute(
                "CREATE TABLE schule(" +
                    "su_kennzahl INT, " +
                    "su_titel VARCHAR(80)," +
                    "su_strasse VARCHAR(80)," +
                    "su_plz INT," +
                    "su_ort VARCHAR(40)" +
                ")"
            );
            System.out.println("Created table schule");

            psInsert = conn.prepareStatement(
                    "insert into schule values (?, ?, ?, ?, ?)");
            statements.add(psInsert);

            psInsert.setInt(1, 321014);
            psInsert.setString(2, "Polytechnische Schule");
            psInsert.setString(3, "Beim Heisselgarten 24");
            psInsert.setInt(4, 3430);
            psInsert.setString(5, "Tulln");
            psInsert.executeUpdate();

            psInsert.setInt(1, 321418);
            psInsert.setString(2, "Handelsakademie und Handelsschule der Stadtgemeinde Tulln ");
            psInsert.setString(3, "Donaulände 64");
            psInsert.setInt(4, 3430);
            psInsert.setString(5, "Tulln");
            psInsert.executeUpdate();
            System.out.println("Inserted data");
        } catch (SQLException sqle) {
            printSQLException(sqle);
        }
    }

    void send(AmqpSender amqpSender) throws IOException, TimeoutException {
        ResultSet rs = null;
        try {
            rs = s.executeQuery(
                    "SELECT * FROM schule ORDER BY su_kennzahl");

            while(rs.next()) {
                String json = String.format(
                    "{\n" +
                    "  \"su_kennzahl\": %1$d,\n" +
                    "  \"su_ort\": \"%2$s\"\n" +
                    "  \"su_strasse\": \"%2$s\"\n" +
                    "  \"su_plz\": \"%2$s\"\n" +
                    "  \"su_ort\": \"%2$s\"\n" +
                    "}",
                    rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getString(5));
                amqpSender.send(json);
            }

            try {
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch (SQLException se) {
                if (((se.getErrorCode() == 50000)
                        && ("XJ015".equals(se.getSQLState())))) {  // we got the expected exception
                    System.out.println("Derby shut down normally");
                    // Note that for single database shutdown, the expected
                    // SQL state is "08006", and the error code is 45000.
                } else {
                    System.err.println("Derby did not shut down normally");
                    printSQLException(se);
                }
            }
        } catch (SQLException sqle) {
            printSQLException(sqle);
        }  finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }
        }
    }

    public static void printSQLException(SQLException e) {
        while (e != null) {
            System.err.println("\n----- SQLException -----");
            System.err.println("  SQL State:  " + e.getSQLState());
            System.err.println("  Error Code: " + e.getErrorCode());
            System.err.println("  Message:    " + e.getMessage());
            e = e.getNextException();
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        AmqpSender amqpSender = new AmqpSender("sokrates_updates_test2");
        Schule schule = new Schule();
        schule.setupDatabase(args);
        System.out.println("Sending to queue");
        schule.send(amqpSender);
        System.out.println("Reading from queue");
        while(true) {
            String msg = amqpSender.receive();
            if (msg == null) {
                break;
            } else {
                System.out.println(msg);
            }
        }
        amqpSender.close();
    }
}
