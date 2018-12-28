package org.hrong.sparkdemo.kafka;

import org.hrong.sparkdemo.constants.ModuleConstants;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName ProducerTest
 * @Author hrong
 * @Date 2018/12/28 17:38
 * @Description
 * @Version 1.0
 **/
public class ProducerTest {
	private static ExecutorService pool = Executors.newFixedThreadPool(10);
	//Initiating connection to node 0 at s101:9092.
	public static void main(String[] args) {
		for (int i = 0; i < 3; i++) {
			pool.execute(new CustomerKafkaProducer(ModuleConstants.KAFKA_TOPIC, false, 5));
		}
	}
}
