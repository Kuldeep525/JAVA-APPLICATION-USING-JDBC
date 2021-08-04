package com.blc.program;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import com.connection.SqlConnection;

public class SqlStatement {
		Scanner sc = new Scanner(System.in);
	public static  void main(String[] args) {
		SqlStatement sqlStatement = new SqlStatement();
		SqlConnection sqlConnection  = new SqlConnection();
		Connection connection = sqlConnection.connection();
		sqlStatement.createDatabase(connection);
		sqlStatement.createTable(connection);
		sqlStatement.insertIntoEmployee(connection);
		sqlStatement.selectAll(connection); 
		sqlStatement.selectedData(connection);
		sqlStatement.addColumn(connection);
		sqlStatement.updateGender(connection);
		sqlStatement.updateSalary(connection);
		sqlStatement.showTableByDate(connection);
		sqlStatement.findMax(connection);
		sqlStatement.findMin(connection);
		sqlStatement.findSum(connection);
		sqlStatement.findAvg(connection);
		sqlStatement.count(connection);
		sqlStatement.insertMultiQuery(connection);
		sqlStatement.insertInERTables(connection);
		sqlStatement.removeRow(connection);
	}


	private void createDatabase(Connection connection) {
		
		String query = "Create DataBase PayRollService";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.execute();
			ps.close();
			connection.close();
			System.out.println("Database created");
		} catch (SQLException e) {	
			e.printStackTrace();
		}		
	}
	
	private void createTable(Connection connection) {
		
		String query = "Create table Employee_payRoll(empId int(10) , eName varchar(50) , salary dec(10,2) , startDate date); ";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.execute();
			ps.close();
			connection.close();
			System.out.println("table created");
		} catch (SQLException e) {	
			e.printStackTrace();
		}		
	}
	
	private void insertIntoEmployee(Connection connection) {
		
		System.out.println("Enter the Id");
		int empid = sc.nextInt();
		System.out.println("Enter the name ");
		String eName = sc.next();
		System.out.println("Enter the gender");
		String gender = sc.next();
		System.out.println("Enter the Salary");
		double salary = sc.nextDouble();
		System.out.println(" Enter the start Date ");
		String date = sc.next();
  
		
		String query = "Insert into employee_payroll(empid , eName , gender,  salary , startDate) values(?,?,?,?,?)";
		PreparedStatement ps;
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, empid);
			ps.setString(2, eName);
			ps.setString(3, gender);
			ps.setDouble(4,salary);
			ps.setString(5, date);
			ps.execute();
			ps.close();
			connection.close();
			System.out.println("Data insert");
		} catch (SQLException e) {
			
			e.printStackTrace();
		}	
	}
	
	private void selectAll(Connection connection) {
		
		String query = "Select * from employee_payroll";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ResultSet set = ps.executeQuery(query);
			while(set.next()) {
				System.out.println(set.getInt(1) + " "+ set.getString(2) + " " + set.getDouble(3) + " " + set.getDate(4));
			}
			ps.execute();
			ps.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void selectedData(Connection connection) {     // doubt 
		System.out.println("Enter the name");
		String name = sc.next();
		
		String query = "Select * from employee_payroll where eName = ?";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, name);
			ResultSet set = ps.executeQuery();
			while(set.next()) {
				System.out.println(set.getInt(1) + " "+ set.getString(2) + " " + set.getString(3) +" "+ set.getDouble(4) + " " + set.getDate(5));
			}
			ps.execute();
			ps.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void addColumn(Connection connection) {
		
		String query = "Alter table employee_payroll add column gender char(1) after ename ";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.execute();
			ps.close();
			connection.close();
			System.out.println("Column added");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

	private void updateGender(Connection connection) {
		
		System.out.println("Enter the gender");
		String gender = sc.next();
		System.out.println("Ente the id");
		int id = sc.nextInt();
		
		String query = "update employee_payroll Set gender = ? where Empid = ? ";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, gender);
			ps.setInt(2, id);
			ps.execute();
			ps.close();
			connection.close();
			System.out.println(" added");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	private void updateSalary(Connection connection) {
			System.out.println("Enter the name ");
			String name = sc.next();
			System.out.println("Enter the Salary ");
			double salary = sc.nextDouble();
			
			String query  = "update employee_payroll Set salary = ? where Ename = ? ";
			
			try {
				
				PreparedStatement ps = connection.prepareStatement(query);
				ps.setString(2, name);
				ps.setDouble(1, salary);
				ps.execute();
				ps.close();
				connection.close();
				System.out.println("Data updated");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	}
	
	private void showTableByDate(Connection connection) {
		System.out.println("Enter the start date ");
		String date = sc.next();
		System.out.println("Enter the end date ");
		String endDate = sc.next();
		
		String query = "Select  * from employee_payroll where Startdate between ? and ?";
	
		try {			
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, date);
			ps.setString(2, endDate);
			ResultSet set = ps.executeQuery();
			while(set.next()) {
				System.out.println(set.getInt(1) + " "+ set.getString(2) + " " + set.getString(3) +" "+ set.getDouble(4) + " " + "joining date:: "+ set.getDate(5));
			}
			ps.close();
			connection.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
//	---------------------find Max , Min , Sum , Avg-----------------------------------------
	
	private void findMax(Connection connection) {
		
		System.out.println("Enter the gender");
		String gender = sc.next();
		String query = "select max(Salary) from employee_payroll where Gender = ? Group by gender ";
		
		
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, gender);
			ResultSet set = ps.executeQuery();
			set.next();
			Double salary = set.getDouble(1);
			System.out.println("Maximum salary is :: " +salary);
			ps.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void findMin(Connection connection) {
		
		System.out.println("Enter the gender");
		String gender = sc.next();
		String query = "select min(Salary) from employee_payroll where Gender = ? Group by gender ";
		
		
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, gender);
			ResultSet set = ps.executeQuery();
			set.next();
			Double salary = set.getDouble(1);
			System.out.println("Mainimum salary is :: " + salary);
			ps.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void findSum(Connection connection) {
		
		System.out.println("Enter the gender");
		String gender = sc.next();
		String query = "select Sum(Salary) from employee_payroll where Gender = ? Group by gender ";
		
		
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, gender);
			ResultSet set = ps.executeQuery();
			set.next();
			Double salary = set.getDouble(1);
			System.out.println(salary);
			ps.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void findAvg(Connection connection) {
		
		System.out.println("Enter the gender");
		String gender = sc.next();
		String query = "select Avg(Salary) from employee_payroll where Gender = ? Group by gender ";
		
		
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, gender);
			ResultSet set = ps.executeQuery();
			set.next();
			Double salary = set.getDouble(1);
			System.out.println(salary);
			ps.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	private void count(Connection connection) {
		
		System.out.println("Enter the gender");
		String gender = sc.next();
		String query = "select count(Salary) from employee_payroll where Gender = ? Group by gender ";
		
		
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			ps.setString(1, gender);
			ResultSet set = ps.executeQuery();
			set.next();
			int count = set.getInt(1);
			System.out.println("Count is :: " + count );
			ps.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/* -------------------------------- SINGLE TRANSCATION Using Autocommit and rollback method-------------*/
	
	private void insertMultiQuery(Connection connection) {
		
		try {
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			statement.executeUpdate("Insert into employee2 values(5 , 'Rajesh' , 'Valsad' , 22770.00);");
			statement.executeUpdate("Insert into employee2 values(6 , 'raj' , 'pardi' , 19670.00);");
			connection.commit();
			statement.close();
			connection.close();
			
			System.out.println("Data inserted");
					
		} catch (SQLException e) {	
			e.printStackTrace();
			try {
				connection.rollback();
				System.out.println("data rollback");
			} catch (SQLException e1) {	
				e1.printStackTrace();
			}
		}	
	}
	private void insertInERTables(Connection connection) {
		
		try {
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			statement.executeUpdate("insert into payRoll values (1005 , 10000.00 , 9900.00 , 100.00);");
			statement.executeUpdate("Insert into department values(2005 , 'Web-Developer' , 104 , 1001);");
			statement.execute("Select * from Company Left Join Employee On Company.companyId = Employee.companyid;");
			connection.commit();
			statement.close();
			connection.close();
			System.out.println("All done");
			
			System.out.println("Data inserted");
					
		} catch (SQLException e) {	
			e.printStackTrace();
			try {
				connection.rollback();
				System.out.println("data rollback");
			} catch (SQLException e1) {	
				e1.printStackTrace();
			}
		}	
	}
	
	private void removeRow(Connection connection) {
		
		try {
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			statement.executeUpdate("Delete from employee2 where name = 'raj'");
			
			connection.commit();
			statement.close();
			connection.close();
			
			System.out.println("Data deleted");
					
		} catch (SQLException e) {	
			e.printStackTrace();
			try {
				connection.rollback();
				System.out.println("data rollback");
			} catch (SQLException e1) {	
				e1.printStackTrace();
			}
		}	
	}
		
}
