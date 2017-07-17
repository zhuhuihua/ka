package com.time.before.kafka.rest.builder;

public class Info{
		
		public int broker_id;
		public String host;
		public int port;
		
		public Info(){};
		public Info(int id, String host, int port){
			this.broker_id = id;
			this.host = host;
			this.port = port;
		}
	}