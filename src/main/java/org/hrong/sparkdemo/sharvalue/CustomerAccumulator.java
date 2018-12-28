package org.hrong.sparkdemo.sharvalue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

/**
 * @ClassName CustomerAccumulator
 * @Author hrong
 * @Date 2018/12/27 17:07
 * @Description 自定义累加器
 * @Version 1.0
 **/
public class CustomerAccumulator {
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder()
				.master("local[4]")
				.appName("CustomerBroadcast")
				.getOrCreate();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		sc.setLogLevel("ERROR");
		JavaRDD<String> rdd = sc.parallelize(Arrays.asList("ONE", "TWO", "THREE","ONE"));
		UserDefinedAccumulator count = new UserDefinedAccumulator();
		//将累加器进行注册
		sc.sc().register(count, "user_count");
		//随机设置值
		JavaPairRDD<String, String> pairRDD = rdd.mapToPair((PairFunction<String, String, String>) s -> {
			int num = new Random().nextInt(10);
			return new Tuple2<>(s, s + ":" + num);
		});
		//foreach中进行累加
		pairRDD.foreach((VoidFunction<Tuple2<String, String>>) tuple2 -> {
			System.out.println(tuple2._2);
			count.add(tuple2._2);
		});
		System.out.println("the value of accumulator is:"+count.value());
	}
}
