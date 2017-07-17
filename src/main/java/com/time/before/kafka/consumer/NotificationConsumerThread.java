package com.time.before.kafka.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.time.before.kafka.hbase.HbaseUtil;

public class NotificationConsumerThread implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final String topic;

//	private static final Logger logger = LogManager.getLogger(NotificationConsumerThread.class.getSimpleName());

	public NotificationConsumerThread(String brokers, String groupId, String topic) {
		Properties prop = createConsumerConfig(brokers, groupId);
		this.consumer = new KafkaConsumer<>(prop);
		this.topic = topic;
		this.consumer.subscribe(Arrays.asList(this.topic));
	}

	private static Properties createConsumerConfig(String brokers, String groupId) {

		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	@Override
	public void run() {
		
//		List<String> person_infos = new ArrayList<String>();

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(10000);
			for(ConsumerRecord<String, String> record : records){
				
				System.out.println(record.value());
				
//				person_infos.add(record.value());
//				
//				if(person_infos.size() / 1000 == 0){
//					try {
//						HbaseDao.putData(TableName.valueOf("person_info"), "info", "", person_infos);
//					} catch (IOException e) {
//						logger.error(e);
//					}finally{
//						person_infos.clear();
//					}
//				}
			}
		}

	}

}