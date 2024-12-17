package jdbccse;

import java.sql.*;

public class demo {

	public static void main(String[] args) {
		batchdemo();
	}
	public static void readrecords()
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		String query="select * from employee";
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			//System.out.println("Conection is Successfull");
			Statement st= con.createStatement();
			ResultSet rs=st.executeQuery(query);
			while(rs.next()) {
			System.out.println("Id is "+rs.getInt(1));
			System.out.println("Name is "+rs.getString(2));
			System.out.println("Salary is "+rs.getInt(3));
			}
			
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	public static void insertrecords()
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		String query="insert into employee values(4,'pyrates',400000);";
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			Statement st= con.createStatement();
			int rows=st.executeUpdate(query);
			System.out.print("Number of rows affected: "+rows);
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	
	public static void insertusingpst()
	// here using prepared statement
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		String query="insert into employee values (?,?,?);";
		int id = 5;
		String name="vichard";
		int salary=300000;
		
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			PreparedStatement pst= con.prepareStatement(query);
			pst.setInt(1, id);
			pst.setString(2,name);
			pst.setInt(3, salary);
			int rows=pst.executeUpdate();
			
			System.out.print("Number of rows affected : "+rows);
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	
	public static void delete()
	// here also using preparedstatement
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		int id = 5;
		String query="delete from employee where emp_id=?";
		
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			PreparedStatement pst= con.prepareStatement(query);
			pst.setInt(1, id);
			int rows=pst.executeUpdate();
			
			System.out.print("Number of rows affected : "+rows);
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	
	public static void update()
	// here also using preparedstatement
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		int id=1;
		String query="update employee set salary=150000 where emp_id=?";
		
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			PreparedStatement pst= con.prepareStatement(query);
			pst.setInt(1, id);
			int rows=pst.executeUpdate();
			
			System.out.print("Number of rows affected : "+rows);
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	
	//calling simple stored procedure without parameter 
	public static void sp()
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			CallableStatement cst= con.prepareCall("{call GetEmp()}");
			ResultSet rs=cst.executeQuery();
			while(rs.next()) {
			System.out.println("Id is "+rs.getInt(1));
			System.out.println("Name is "+rs.getString(2));
			System.out.println("Salary is "+rs.getInt(3));
			}
			
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	
	//calling simple stored procedure with parameter
	public static void sp2()
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		int id=3;
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			CallableStatement cst= con.prepareCall("{call GetEmpById(?)}");
			cst.setInt(1,id);
			ResultSet rs=cst.executeQuery();
			
			while(rs.next()) {
			System.out.println("Id is "+rs.getInt(1));
			System.out.println("Name is "+rs.getString(2));
			System.out.println("Salary is "+rs.getInt(3));
			}
			
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	
	//calling simple stored procedure with in and out parameter
	public static void sp3()
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		int id=3;
		
		try {
			Connection con=DriverManager.getConnection(url,username,password);
			CallableStatement cst= con.prepareCall("{call GetNameEmpById(?,?)}");
			cst.setInt(1,id);
			cst.registerOutParameter(2, Types.VARCHAR);
			cst.executeUpdate();
			System.out.println(cst.getString(2));
			
			
			con.close();
		}
		catch(Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	
	//commit vs auto-commit
	public static void commitdemo()
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		String query1="update employee set salary =450000 where emp_id=1";
		String query2="update employee set salary =450000 where emp_id=2";

		try {
			Connection con=DriverManager.getConnection(url,username,password);
			con.setAutoCommit(false);
			Statement st=con.createStatement();
			int rows1=st.executeUpdate(query1);
			System.out.println("Rows affected "+rows1);
			int rows2=st.executeUpdate(query2);
			System.out.println("Rows affected "+rows2);
			
			if(rows1>0 && rows2>0 )
				con.commit();
			
			con.close();
		}
		catch (Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}
	//batch processing
	public static void batchdemo()
	{
		String url="jdbc:mysql://localhost:3306/jdbcdemo";
		String username="root";
		String password="Ebinesh$5";
		String query1="update employee set salary =4300000 where emp_id=1";
		String query2="update employee set salary =4300000 where emp_id=2";
		String query3="update employee set salary =4300000 where emp_id=3";
		String query4="update employee set salary =4300000 where emp_id=4";

		try {
			Connection con=DriverManager.getConnection(url,username,password);
			con.setAutoCommit(false);
			Statement st=con.createStatement();
			st.addBatch(query1);
			st.addBatch(query2);
			st.addBatch(query3);
			st.addBatch(query4);
			
			int[] res=st.executeBatch();
			
			for(int i:res)
			{
				if(i>0)
					continue;
				else
					con.rollback();
			}
			con.commit();
			System.out.print("Successfull");
				
			con.close();
		}
		catch (Exception e) {
			System.out.println("Error msg :"+e.getMessage());
		}

	}


}
