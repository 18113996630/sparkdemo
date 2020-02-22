package org.hrong.sparkdemo.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * @author hrong
 **/
public class ConnectionPool {
	private static LinkedList<Connection> pool = new LinkedList<>();
	private static final int MAX_CONNECTION = 10;
	private static final String URL = "jdbc:mysql://localhost:3306/test";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "123456";
	/**
	 * 获取连接
	 */
	public static Connection getConnection(){
		while (pool.size() < MAX_CONNECTION) {
			Connection connection = getConnectionFromDb();
			if (connection != null) {
				pool.add(connection);
			}
		}
		return pool.removeFirst();
	}

	public static void returnConnection(Connection connection){
		pool.addLast(connection);
	}

	private static Connection getConnectionFromDb(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection(URL, USERNAME, PASSWORD);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	public static void main(String[] args) {
		System.out.println(pool.size());
		Connection connection = getConnection();
		System.out.println(pool.size());
		returnConnection(connection);
		System.out.println(pool.size());
	}

}
