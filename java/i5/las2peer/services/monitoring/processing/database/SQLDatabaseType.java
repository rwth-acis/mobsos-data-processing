package i5.las2peer.services.monitoring.processing.database;


/**
 * 
 * SQLDatabaseType.java
 * <br>
 * Enumeration class that provides the right drivers according for the database type.
 * The original code was taken from the QueryVisualizationService.
 * 
 * This implementation only supports DB2 and MySQL, since those are that were tested with this service.
 * 
 * @author Peter de Lange
 * 
 */
public enum SQLDatabaseType {
	
	// db2jcc_javax-0.jar
	DB2 (1),
	// mysql-connector-java-5.1.16.jar (MySQL 5.1)
	MySQL (2);
	
	private final int code;
	
	
	SQLDatabaseType(int code){
		this.code = code;
	}
	
	
	public int getCode(){
		return this.code;
	}
	
	
	/**
	 * Returns the database type.
	 * 
	 * @param code
	 * @return the type
	 */
	public static SQLDatabaseType getSQLDatabaseType(int code){
		switch(code){
			case 1:
				return SQLDatabaseType.DB2;
			case 2:
				return SQLDatabaseType.MySQL;
		}
		return null;
	}
	
	
	/**
	 * 
	 * Returns the driver name of the corresponding database.
	 * The library of this driver has to be in the lib folder.
	 * 
	 * @return a driver name
	 * 
	 */
	public String getDriverName(){
		switch(this.code){
			case 1:
				return "com.ibm.db2.jcc.DB2Driver";
			case 2:
				return "com.mysql.jdbc.Driver";
		}
		return null;
	}
	
	
	/**
	 * 
	 * Adds the URL prefix.
	 * 
	 * @param host
	 * @param database
	 * @param port
	 * 
	 * @return a String representing the JDBC Curl
	 * 
	 */
	public String getJDBCurl(String host, String database, int port){
		String url = null;
		switch(this.code){
			case 1:
				url = "jdbc:db2://" + host + ":" + port + "/" + database;
				break;
			case 2:
				url = "jdbc:mysql://" + host + ":" + port + "/" + database;
				break;
			default:
				return null;
		}
		return url;
	}
	
	
}
