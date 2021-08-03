package com.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlConnection {
		
	String url = "jdbc:mysql://localhost:3306/payrollservice" ;
	String userName = "root" ;
	String password = "Kuldeep725@";
		
	public Connection connection() {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url ,userName , password );
			return connection;
				
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
			
	}
}
