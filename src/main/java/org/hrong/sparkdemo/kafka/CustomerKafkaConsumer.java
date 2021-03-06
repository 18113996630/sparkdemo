package org.hrong.sparkdemo.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.hrong.sparkdemo.kafka.vo.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName CustomerKafkaConsumer
 * @Author hrong
 * @Date 2018/12/28 15:04
 * @Description
 * @Version 1.0
 **/
public class CustomerKafkaConsumer implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(CustomerKafkaConsumer.class);
	private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private static FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
	public static boolean FLAG = true;
	private String topic;
	private static SparkSession sparkSession = null;
	private static JavaSparkContext sc = null;
	private static JavaStreamingContext jsc = null;


	public CustomerKafkaConsumer(String topic, Integer duration) {
		this.topic = topic;
		getInstance(duration);
		jsc.sparkContext().setLogLevel("WARN");
	}

	public static void getInstance(Integer duration) {
		if (sparkSession == null) {
			sparkSession = SparkSession.builder()
					.appName("CustomerKafkaProducer")
					.master("local[3]")
					.getOrCreate();
		}
		if (sc == null) {
			sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		}
		if (jsc == null) {
			jsc = new JavaStreamingContext(sc, Seconds.apply(duration));
		}
	}

	@Override
	public void run() {
		try {
			consumeMessage();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	public void consumeMessage() throws InterruptedException {
		//查看org.apache.spark.streaming.kafka.KafkaCluster
//		jsc.checkpoint(ModuleConstants.BASE_PATH);
		Map<String, Object> kafkaParams = new HashMap<>();
		String broker = getParamsFromConfig("bootstrap.servers");
//		String offset = getParamsFromConfig("auto.offset.reset");
		kafkaParams.put("bootstrap.servers", broker);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id" , "test_group");
		Collection<String> topics = new HashSet<>();
		topics.add(topic);
		//创建inputDStream
		JavaPairInputDStream<String, String> dStream = null;
		JavaInputDStream<ConsumerRecord<Object, Object>> directStream = KafkaUtils
				.createDirectStream(jsc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.Subscribe(topics, kafkaParams));
		JavaDStream<KafkaMessage> stream = directStream.map((Function<ConsumerRecord<Object, Object>, KafkaMessage>) v1 -> {
			String message = String.valueOf(v1.value());
			KafkaMessage vo = JSON.parseObject(message, KafkaMessage.class);
			return vo;
		});
		stream.print();
		if (FLAG) {
			jsc.start();
			jsc.awaitTermination();
			FLAG = false;
		}
	}

	public static String getParamsFromConfig(String key) {
		InputStream inputStream = CustomerKafkaConsumer.class.getClassLoader().getResourceAsStream("kafka.properties");
		Properties properties = new Properties();
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties.getProperty(key);
	}


}
