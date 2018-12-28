package org.hrong.sparkdemo.kafka;

import org.hrong.sparkdemo.constants.ModuleConstants;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName ConsumerTest
 * @Author hrong
 * @Date 2018/12/28 18:18
 * @Description
 * @Version 1.0
 **/
public class ConsumerTest {
	private static ExecutorService pool = Executors.newFixedThreadPool(10);
	public static void main(String[] args) throws InterruptedException {
		for (int i = 0; i < 1; i++) {
			pool.execute(new CustomerKafkaConsumer(ModuleConstants.KAFKA_TOPIC, 3));
		}
	}
}
