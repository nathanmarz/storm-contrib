package storm.contrib.rdbms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/*
 * Class implementing methods for communicating(create a table, check for a table's existence, insert a row) with RDBMS
 */
public class RDBMSCommunicator {

	private Connection con = null;
	private ResultSet rs = null;
	private PreparedStatement prepstmt = null;
	private boolean result = false;
	private String stmt = null, colType = null, values = "", primaryKey = null, tableName = null;
	private int r = 0, noOfColumns = 0;
	private ArrayList<String> columnNames = new ArrayList<String>(); 
	private ArrayList<String> columnTypes = new ArrayList<String>();

	public RDBMSCommunicator(Connection con, String primaryKey, String tableName,
			ArrayList<String> columnNames, ArrayList<String> columnTypes) {
		super();
		this.primaryKey = primaryKey;
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.con = con;

		//check if the table exists 
		if(!tableExists(tableName)) {
			try {
				createTable(tableName, primaryKey, columnNames, columnTypes);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	//check for table's existence 
	public boolean tableExists(String tableName) {
		try {		
			prepstmt = null;
			stmt = "select * from " + tableName;
			prepstmt = con.prepareStatement(stmt);
			rs = prepstmt.executeQuery();
			if(rs.next()) {
				result = true;
			} else {
				result = false;				
			}
		} catch(Exception e) {
			result = false;
		}
		return result;
	}

	//create a table in RDBMS
	public void createTable(String tableName, String primaryKey, ArrayList<String> columnNames, ArrayList<String> columnTypes) throws SQLException {
		try {		
			prepstmt = null;
			stmt = "CREATE TABLE " + tableName + "(";
			noOfColumns = columnNames.size();
			colType = null;
			if(columnNames.size() == columnTypes.size()) {
				for(int i = 0; i < noOfColumns - 1; i++) {
					colType = columnTypes.get(i);
					stmt = stmt + columnNames.get(i) + " " + colType + ",";	
				}
				stmt = stmt + columnNames.get(noOfColumns-1) + " " + columnTypes.get(noOfColumns-1);

			} else {
				System.out.println("Wrong input : Number of columns doesn't match the number of given data types");
			}
			if(!primaryKey.equals("N/A")) {
				stmt = stmt + ", PRIMARY KEY (" + primaryKey + "))";
			} else {
				stmt = stmt + ")";
			}
			prepstmt = con.prepareStatement(stmt);
			r = prepstmt.executeUpdate();
			if(r != 0) {
				return;
			}
				
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	//insert a row in the RDBMS table 
	public int insertRow(ArrayList<Object> fieldValues) throws SQLException {
		int r = 0;
		try {		
			prepstmt = null;
			values = "";
			noOfColumns = columnNames.size(); 
			stmt = "insert into " + tableName + " (";
			for(int i = 0; i <= noOfColumns - 1; i++) {
				if(i != noOfColumns - 1) {
					stmt = stmt + columnNames.get(i) + ", ";
					values = values + "?,";
				}
				else {
					stmt = stmt + columnNames.get(i) + ") ";
					values = values + "?";
				}
			}
			stmt = stmt + " values (" +  values + ")";
			prepstmt = con.prepareStatement(stmt);
			for(int j = 0; j <= noOfColumns - 1; j++) {
				prepstmt.setObject(j + 1, fieldValues.get(j));
			}
			r = prepstmt.executeUpdate();
			if(r == 0) {
				return 0;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return r;
	}
}
