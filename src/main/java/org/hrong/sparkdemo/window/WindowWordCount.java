package org.hrong.sparkdemo.window;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.hrong.sparkdemo.sink.MysqlSink;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @author hrong
 **/
public class WindowWordCount {
//	private static MysqlSink<Row> mysqlSink = null;
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("WindowWordCount")
				.master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		Dataset<Row> data = spark.readStream()
				.format("socket")
				.option("host", "n151")
				.option("port", "19999")
				.load();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Dataset<Row> wordCounts = data.as(Encoders.STRING()).map((MapFunction<String, Tuple2<Timestamp, String>>) row -> {
			String[] datas = row.split(",");
			Timestamp time = new Timestamp(format.parse(datas[0]).getTime());
			String word = datas[1];
			return new Tuple2<>(time, word);
		}, Encoders.tuple(Encoders.TIMESTAMP(), Encoders.STRING())).toDF("ts", "word");
		Dataset<Row> result = wordCounts.groupBy(functions.window(wordCounts.col("ts"), "10 seconds", "5 seconds"),
				wordCounts.col("word")
		).count();
		MysqlSink<Row> mysqlSink = new MysqlSink<>();
		StreamingQuery query = result.writeStream()
				.outputMode("complete")
				.format("console")
				.trigger(Trigger.ProcessingTime(0))
				.option("truncate", false)
				.foreach(mysqlSink)
				.start();
		try {
		    query.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}

		/**
		 * 输入：
		 * 2020-02-20 20:10:00,零
		 * 2020-02-20 20:10:10,十
		 * 2020-02-20 20:10:15,十五
		 * 2020-02-20 20:10:14,十四
		 * 2020-02-20 20:10:14,十四
		 *
		 * 输出：
		 * +---------------------------------------------+----+-----+
		 * |window                                       |word|count|
		 * +---------------------------------------------+----+-----+
		 * |[2020-02-20 20:10:05.0,2020-02-20 20:10:15.0]|十四  |2    |
		 * |[2020-02-20 20:10:05.0,2020-02-20 20:10:15.0]|十   |1    |
		 * |[2020-02-20 20:10:00.0,2020-02-20 20:10:10.0]|零   |1    |
		 * |[2020-02-20 20:10:10.0,2020-02-20 20:10:20.0]|十四  |2    |
		 * |[2020-02-20 20:10:15.0,2020-02-20 20:10:25.0]|十五  |1    |
		 * |[2020-02-20 20:09:55.0,2020-02-20 20:10:05.0]|零   |1    |
		 * |[2020-02-20 20:10:10.0,2020-02-20 20:10:20.0]|十五  |1    |
		 * |[2020-02-20 20:10:10.0,2020-02-20 20:10:20.0]|十   |1    |
		 * +---------------------------------------------+----+-----+
		 */
	}
}
