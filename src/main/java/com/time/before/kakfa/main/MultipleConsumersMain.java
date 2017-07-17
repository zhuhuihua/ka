package com.time.before.kakfa.main;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;

import com.time.before.kafka.consumer.NotificationConsumerGroup;
import com.time.before.kafka.hbase.HbaseUtil;
import com.time.before.kakfa.producer.NotificationProducerThread;

/**
 * 主启动类
 * 
 * @author u
 *
 */

public final class MultipleConsumersMain {

	public static void main(String[] args) throws IOException {
		
//		HbaseDao.createTable(TableName.valueOf("person_info"), new String[]{"info"});
		
		String brokers_url = "192.168.106.10:9092";
		String groupId = "group01";
		String topic = "my-topic";
		int numberOfConsumer = 3;

		if (args != null && args.length > 4) {
			brokers_url = args[0];
			groupId = args[1];
			topic = args[2];
			numberOfConsumer = Integer.parseInt(args[3]);
		}

		// Start Notification Producer Thread
//		NotificationProducerThread producerThread = new NotificationProducerThread(brokers_url, topic);
//		Thread t1 = new Thread(producerThread);
//		t1.start();

		// Start group of Notification Consumers
		NotificationConsumerGroup consumerGroup = new NotificationConsumerGroup(brokers_url, groupId, topic,
				numberOfConsumer);

		consumerGroup.execute();

		try {
			Thread.sleep(100000);
		} catch (InterruptedException ie) {

		}
	}
}