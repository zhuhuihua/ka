package com.time.before.kafka.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.time.before.kafka.properties.KafkaProperties;

@SpringBootApplication
@ComponentScan("com.dk")
public class KafkaManager{
	
	@Autowired
	private KafkaProperties kp;
	
	//log4j-over-slf4j,logback*.jar, servlet2.5.jar
	public static void main(String args[]){
		
		SpringApplication.run(KafkaManager.class, args);
		
	}
}
