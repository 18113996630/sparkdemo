package org.hrong.sparkdemo.rddaction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.hrong.sparkdemo.constants.ModuleConstants;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @ClassName RddAction
 * @Author hrong
 * @Date 2018/12/27 17:18
 * @Description rdd基础操作
 * @Version 1.0
 **/
public class RddAction {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder()
				.master("local[4]")
				.appName("CustomerBroadcast")
				.getOrCreate();
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		JavaRDD<String> javaRDD = sc.textFile(ModuleConstants.BASE_PATH +"rddaction.txt");
		//将文本文件中的数进行拆分
		JavaRDD<String> numberRDD = javaRDD.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(",")).iterator());
		//将每个数变为原来的2倍
		JavaRDD<Integer> mapRDD = numberRDD.map((Function<String, Integer>) v1 -> Integer.valueOf(v1) * 2);
		/**
		 *	计算每个数字出现的次数
		 */
		//将每个数字变成 num,1 的形式
		JavaPairRDD<Integer, Integer> number_1_RDD = mapRDD.mapToPair((PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, 1));
		//针对key相同的rdd进行value相加的操作
		JavaPairRDD<Integer, Integer> numberCountRDD = number_1_RDD.reduceByKey((Function2<Integer, Integer, Integer>) (value1, value2) -> value1 + value2);
		//持久化RDD
		numberCountRDD = numberCountRDD.persist(StorageLevel.MEMORY_ONLY());
		numberCountRDD.cache();
		numberCountRDD.foreach((VoidFunction<Tuple2<Integer, Integer>>) integerIntegerTuple2 -> {
			System.out.println(integerIntegerTuple2._1+"出现的次数："+integerIntegerTuple2._2);
		});
		/**
		 * 找出出现次数最多的数字
		 */
		//先交换number-count
		JavaPairRDD<Integer, Integer> countNumberRDD = numberCountRDD.mapToPair((PairFunction<Tuple2<Integer, Integer>, Integer, Integer>) integerIntegerTuple2 ->
				new Tuple2<>(integerIntegerTuple2._2, integerIntegerTuple2._1));
		Tuple2<Integer, Integer> CountTop1 = countNumberRDD.sortByKey(false).take(1).get(0);
		System.out.println(CountTop1._2+"出现的次数最多，次数为："+CountTop1._1);
	}
}
