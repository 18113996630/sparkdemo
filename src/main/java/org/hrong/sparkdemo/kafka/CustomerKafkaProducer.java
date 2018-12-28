package org.hrong.sparkdemo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.hrong.sparkdemo.constants.ModuleConstants;
import org.hrong.sparkdemo.kafka.vo.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * kafkaProducer 是线程安全，可以使用单例模式
 *
 * @ClassName CustomerKafkaProducer
 * @Author hrong
 * @Date 2018/12/28 15:03
 * @Description
 * @Version 1.0
 **/
public class CustomerKafkaProducer implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(CustomerKafkaProducer.class);
	private String topic;
	private KafkaProducer<String, String> producer;
	private long waite;
	private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	//是否异步发送
	private Boolean isAsync = false;

	/**
	 * 创建对象
	 * @param topic 主题
	 * @param isAsync 是否异步
	 * @param waite 每条消息间隔时间
	 */
	public CustomerKafkaProducer(String topic, boolean isAsync, long waite) {
		this.topic = topic;
		this.waite = waite * 1000;
		this.isAsync = isAsync;
		Properties properties = new Properties();
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("kafka.properties");
		try {
			properties.load(inputStream);
			producer = new KafkaProducer<>(properties);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void sendMessage(long waite) {
		int num = 0;
		while (true) {
			long _start = System.currentTimeMillis();
			String threadName = Thread.currentThread().getName();
			String date = LocalDateTime.now().format(formatter);
			String message = JSON.toJSONString(new KafkaMessage(threadName,num,date));
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, threadName, message);
			if (isAsync) {
				logger.info("开始异步发送消息：" + message);
				producer.send(record, (metadata, exception) -> {
					if (metadata != null && exception == null) {
						long offset = metadata.offset();
						int partition = metadata.partition();
						String topic = metadata.topic();
						long asyncTime = System.currentTimeMillis();
						logger.info("发送消息：" +message+",发送花费时间："+ (asyncTime - _start));
					}
					logger.error("metadata is null~~~~~~~~~~~~~~~~~");
				});
			} else {
				logger.info("开始同步发送消息：" + message);
				Future<RecordMetadata> send = producer.send(record);
				try {
					RecordMetadata metadata = send.get();
					long offset = metadata.offset();
					int partition = metadata.partition();
					String topic = metadata.topic();
					long asyncTime = System.currentTimeMillis();
					logger.info("发送消息：" +message+",发送花费时间："+ (asyncTime - _start));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			num++;
			try {
				Thread.sleep(waite);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		sendMessage(waite);
	}
}
