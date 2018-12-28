package org.hrong.sparkdemo.sharvalue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * @ClassName CustomerBroadcast
 * @Author hrong
 * @Date 2018/12/27 17:08
 * @Description 广播变量
 * @Version 1.0
 **/
public class CustomerBroadcast {
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder()
				.master("local[4]")
				.appName("CustomerBroadcast")
				.getOrCreate();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		Integer num = 2;
		//广播变量是只读的
		Broadcast<Integer> broadcast = sc.broadcast(num);
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5);
		rdd.map((Function<Integer, Integer>) v1 -> v1 + broadcast.value())
			.foreach((VoidFunction<Integer>) integer -> System.out.println(integer));
		System.out.println("broadcast"+broadcast.value());
	}
}
