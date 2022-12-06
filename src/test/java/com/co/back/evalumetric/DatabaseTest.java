package com.co.back.evalumetric;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseTest {

    @Test
    void testConnection () throws SQLException {

	String dbUsername = "linroot";
	String dbPassword = "so2$yUGCSU85qoLr";

	try ( Connection conn = DriverManager.getConnection("jdbc:mysql://lin-10885-6680-mysql-primary.servers.linodedb.net:3306/research", dbUsername, dbPassword); Statement stmt = conn.createStatement() ) {

	    	ResultSet rs   = stmt.executeQuery("SELECT updated_at FROM results;");

		    while (rs.next()) {
			System.out.println(rs.getString("updated_at"));
		    }
	}
    }
}
