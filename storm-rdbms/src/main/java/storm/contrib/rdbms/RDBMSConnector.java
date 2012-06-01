package storm.contrib.rdbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/*
 * Class that connects to the rdbms and returns a Connection object
 */
public class RDBMSConnector {

	private Connection con = null;
	private String dbUrl = null;
	private String dbClass = "com.mysql.jdbc.Driver";
	
	/*
	 * get connection and return a Connection object
	 */
	public Connection getConnection(final String sqlDBUrl, final String sqlUser, final String sqlPassword) throws ClassNotFoundException, SQLException {
		
		StringBuilder builder = new StringBuilder();
		builder.append(sqlDBUrl).append("?user=").append(sqlUser).append("&password=").append(sqlPassword);
		dbUrl = builder.toString();
		Class.forName(dbClass);
		con = DriverManager.getConnection (dbUrl);
		return con;
	}
}
