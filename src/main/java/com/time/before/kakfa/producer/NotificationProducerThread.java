package com.time.before.kakfa.producer;

import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * producer thread
 * 
 * @author u
 *
 */
public class NotificationProducerThread implements Runnable {

//	private static final Logger logger = LogManager.getLogger(NotificationProducerThread.class.getSimpleName());

	private final KafkaProducer<String, String> producer;
	private final String topic;

	public NotificationProducerThread(String brokers, String topic) {

		Properties prop = createProducerConfig(brokers);
		this.producer = new KafkaProducer<String, String>(prop);
		this.topic = topic;
	}

	private static Properties createProducerConfig(String brokers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	@Override
	public void run() {
		
		ProducerRecord<String, String> producerRecord = null;
		Person p = null;
		
		for (int i = 0; i < 5; i++) {
			
			p = new Person("zhuhuihua" + i, i, "male");
			producerRecord = new ProducerRecord<String, String>(topic, p.toString());
			producer.send(producerRecord, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
//						logger.error(e);
						System.out.println(e);
					}
//					logger.info("sent msg:{}, Partition:{}, offset:{}", p.toString(), metadata.partition(), metadata.offset());
				}
			});
		}
		producer.close();
	}
	
	
	public void send(String data){
		
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, data);;
		producer.send(producerRecord, new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
//					logger.error(e);
					System.out.println(e);
				}
//				logger.info("sent msg:{}, Partition:{}, offset:{}", p.toString(), metadata.partition(), metadata.offset());
			}
		});
		
		producer.close();
	}
}

class Person{
	
	public	String id;
	public  String  name;
	public  int age;
	public  String sex;
	
	public Person(String name, int age, String sex){
		
		id = UUID.randomUUID().toString();
		this.name = name;
		this.age = age;
		this.sex = sex;
	}

//	@Override
//	public String toString() {
//		return "Person [id=" + id + ", name=" + name + ", age=" + age + ", sex=" + sex + "]";
//	}
	
	
	@Override
	public String toString(){
		return id + "\t" + name + "\t" + age + "\t" + sex ;
	}
	
}


