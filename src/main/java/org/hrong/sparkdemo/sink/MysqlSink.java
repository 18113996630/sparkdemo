package org.hrong.sparkdemo.sink;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.hrong.sparkdemo.connection.ConnectionPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author hrong
 **/
public class MysqlSink<Row> extends ForeachWriter<Row> {
	private Connection connection = null;


	@Override
	public boolean open(long partitionId, long version) {
		connection = ConnectionPool.getConnection();
		return true;
	}

	@Override
	public void process(Row value) {
		System.out.println("开始处理：");
		System.out.println(value);
	}

	@Override
	public void close(Throwable errorOrNull) {
		if (connection != null) {
			ConnectionPool.returnConnection(connection);
		}
	}
}
