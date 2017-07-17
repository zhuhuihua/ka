package com.time.before.kafka.properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

//@PropertySource("classpath:application.properties")
@Component
public class KafkaProperties {
	
	public KafkaProperties(){}

	@Value("${kafka.host}")
	private String kafka_host;
	@Value("${zk.host}")
	private String zk_host;

	public String getKafka_host() {
		return kafka_host;
	}

	public void setKafka_host(String kafka_host) {
		this.kafka_host = kafka_host;
	}

	public String getZk_host() {
		return zk_host;
	}

	public void setZk_host(String zk_host) {
		System.out.println("zk_host \t" + zk_host );
		this.zk_host = zk_host;
	}

}
