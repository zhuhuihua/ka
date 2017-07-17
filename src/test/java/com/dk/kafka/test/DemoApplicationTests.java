package com.dk.kafka.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.time.before.kafka.properties.KafkaProperties;
import com.time.before.kafka.rest.KafkaManager;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaManager.class)
public class DemoApplicationTests {
	
	@Autowired
	private KafkaProperties kp;
	
	@Test
	public void test() throws Exception {
		System.out.println("host is ..........." + kp.getKafka_host());
	}
}