package com.time.before.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 启动多个consumer消费者线程
 * 
 * @author u
 *
 */
public class NotificationConsumerGroup {

	private final int numberOfConsumers;
	private final String groupId;
	private final String topic;
	private final String brokers;
	private List<NotificationConsumerThread> consumers;
	ExecutorService exector = null;

	public NotificationConsumerGroup(String brokers, String groupId, String topic, int numberOfConsumers) {

		this.brokers = brokers;
		this.topic = topic;
		this.groupId = groupId;
		this.numberOfConsumers = numberOfConsumers;
		consumers = new ArrayList<>();
		exector = Executors.newFixedThreadPool(numberOfConsumers);

		for (int i = 0; i < this.numberOfConsumers; i++) {
			NotificationConsumerThread ncThread = new NotificationConsumerThread(this.brokers, this.groupId,
					this.topic);
			consumers.add(ncThread);
		}
	}

	public void execute() {
		for (NotificationConsumerThread ncThread : consumers) {
			exector.submit(ncThread);
		}
	}

	/**
	 * @return the numberOfConsumers
	 */
	public int getNumberOfConsumers() {
		return numberOfConsumers;
	}

	/**
	 * @return the groupId
	 */
	public String getGroupId() {
		return groupId;
	}

}