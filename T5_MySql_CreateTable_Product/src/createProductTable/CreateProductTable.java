package createProductTable;
//  Can not issue data manipulation statements with executeQuery().


//STEP 1. Import required packages
import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;

public class CreateProductTable {
 // JDBC driver name and database URL
 static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
 static final String DB_URL = "jdbc:mysql://localhost:3306/Test1?autoReconnect=true&useSSL=false";
 // static final String DB_URL = "jdbc:mysql://localhost/Test1";
 // jdbc:mysql://localhost:3306/Peoples?autoReconnect=true&useSSL=false

 //  Database credentials
 static final String USER = "username";
 static final String PASS = "password";
 
 public static void main(String[] args) {
 
	    // ====
	    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
	    System.out.println("Please enter the table name to be created : ");
	    String tablename = null;
	    try {
	        tablename = reader.readLine();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    System.out.println("You entered : " + tablename);	 
	 Connection conn = null;
 Statement stmt = null;

 // Properties properties = new Properties();
 // properties.setProperty("user", USER);
 // properties.setProperty("password", PASS);
 // properties.setProperty("useSSL", "false");
 // properties.setProperty("autoReconnect", "true");
  
 try{
    //STEP 2: Register JDBC driver
    Class.forName("com.mysql.jdbc.Driver");

    //STEP 3: Open a connection
    System.out.println("Connecting to database...");
    conn = DriverManager.getConnection(DB_URL,USER,PASS);

    //STEP 4: Execute a query
    System.out.println("Creating statement...");
    stmt = conn.createStatement();
    String sql;
    
//    tablename = "Products4";

    sql= "drop table if exists " + tablename;
    
    int rs = stmt.executeUpdate(sql);
    System.out.println("Executed drop " + tablename + ", result= "+ rs);

    
    // sql = "SELECT 'Dept Code', Status, 'Product Title' FROM Products";
    sql = "CREATE TABLE " + tablename + " (" 
+"  `InternalID` text, " 
+"  `CategoryIDs` text, " 
+"  `DeptCode` text, " 
+"  `Status` text,"
+"  `ProductTitle` text,  " 
+"  `ShortDescription` text, " 
+"  `UnitOfMeasurement` text,"
+"  `Availability` text,  " 
+"  `UnlimitedInventory` text," 
+"  `Options` text," 
+"  `AssignedOptionValues` text,"
+"  `OptionID` int(11) DEFAULT NULL,  " 
+"  `sku` text,  " 
+"  `upc` text,  " 
+"  `ManufacturerProductId` text,"
+"  `AlternateLookups` text,  " 
+"  `ManufacturerId` text,  " 
+"  `PreferredVendor` text,"
+"  `StoreLocationID` integer,  " 
+"  `Weight` numeric(10,2),"
+"  `Price` numeric(10,2),  " 
+"  `SalePrice` numeric(10,2),  " 
+"  `WholesalePrice` numeric(10,2),"
+"  `Re-OrderPoint` numeric(10), " 
+"  `Re-OrderAmount` numeric(10),  " 
+"  `TaxCode` text,"
+"  `DateAdded` date"
+") ENGINE=InnoDB DEFAULT CHARSET=latin1";
       
     rs = stmt.executeUpdate(sql);
     System.out.println("Executed Create Table " + tablename + ", result= "+ rs);

    //STEP 6: Clean-up environment
    //System.out.println("Clean up environment...");
    stmt.close();
    conn.close();
 }catch(SQLException se){
    //Handle errors for JDBC
    se.printStackTrace();
 }catch(Exception e){
    //Handle errors for Class.forName
    e.printStackTrace();
 }finally{
    //finally block used to close resources
    try{
       if(stmt!=null)
          stmt.close();
    }catch(SQLException se2){
    }// nothing we can do
    try{
       if(conn!=null)
          conn.close();
    }catch(SQLException se){
       se.printStackTrace();
    }//end finally try
 }//end try
 System.out.println("Goodbye!");
}//end main
}//end FirstExample