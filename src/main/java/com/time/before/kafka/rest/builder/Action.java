package com.time.before.kafka.rest.builder;

import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;

public abstract class Action {
	
	protected String topic_name;
	protected String kafka_host;
	protected String zk_host;
	protected String server_host;
	public int noOfReplication;
	public int noOfPartitions;
	
	public String execute(String url) throws Exception{
		
		Content	content = Request.Get(url).execute().returnContent();
		return content.toString();
		
	}
}
