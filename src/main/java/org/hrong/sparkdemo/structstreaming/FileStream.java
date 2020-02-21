package org.hrong.sparkdemo.structstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hrong.sparkdemo.constants.ModuleConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author hrong
 **/
public class FileStream {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName(FileStream.class.getSimpleName())
				.master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("link", DataTypes.StringType, false),
				DataTypes.createStructField("link-href", DataTypes.StringType, false),
				DataTypes.createStructField("title", DataTypes.StringType, false),
				DataTypes.createStructField("type", DataTypes.StringType, false),
				DataTypes.createStructField("date", DataTypes.StringType, false),
				DataTypes.createStructField("view", DataTypes.StringType, false),
				DataTypes.createStructField("dm-count", DataTypes.StringType, false),
				DataTypes.createStructField("like", DataTypes.StringType, false),
				DataTypes.createStructField("coin", DataTypes.StringType, false),
				DataTypes.createStructField("collect", DataTypes.StringType, false)
		);
		StructType type = DataTypes.createStructType(fields);

		Dataset<Row> rowDs = spark.readStream().format("csv").schema(type).option("header", true).load(ModuleConstants.BASE_PATH+"csv");
		try {
			rowDs.createTempView("info");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Dataset<Row> result = spark.sql("select title, like from info");
		StreamingQuery query = result.writeStream().format("console").trigger(Trigger.ProcessingTime(0)).outputMode("append").start();
		try {
			query.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
