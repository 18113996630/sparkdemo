package org.hrong.sparkdemo.kafka.vo;

import java.io.Serializable;

/**
 * @ClassName KafkaMessage
 * @Author hrong
 * @Date 2018/12/28 18:00
 * @Description
 * @Version 1.0
 **/
public class KafkaMessage implements Serializable {
	private static final long serialVersionUID = -757532346826632416L;
	private String threadName;
	private Integer number;
	private String createTime;

	public KafkaMessage() {
	}

	public KafkaMessage(String threadName, Integer number, String createTime) {
		this.threadName = threadName;
		this.number = number;
		this.createTime = createTime;
	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	@Override
	public String toString() {
		return "KafkaMessage{" +
				"threadName='" + threadName + '\'' +
				", number=" + number +
				", createTime='" + createTime + '\'' +
				'}';
	}
}
