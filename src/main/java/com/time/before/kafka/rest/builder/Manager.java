package com.time.before.kafka.rest.builder;

import java.util.List;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class Manager extends Action{
	
	private static final Logger logger = LogManager.getLogger(Manager.class.getSimpleName());
	
	public String zk_host;
	
	protected Manager(Builder builder) {
		this.server_host = builder.server_host;
		this.zk_host = builder.zk_host;
	}
	
	/**
	 * list brokers
	 * @param zk_host
	 * @return
	 */
	public List<Broker> getBrokers(){
		
		 List<Broker> brokers = null;
		 ZkClient zkClient = null;
		 
		try{
		 	final ZkConnection zkConnection = new ZkConnection(zk_host);
	        final int sessionTimeoutMs = 10 * 1000;
	        final int connectionTimeoutMs = 20 * 1000;
	        zkClient = new ZkClient(zk_host, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);

	        final ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
	        brokers = scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
	        scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
	        
		}catch(Exception e){
			logger.error(e);
		}finally{
			if (zkClient != null) {
				zkClient.close();
			}
		}
		
		return brokers;
		
	}
	
	/**
	 * list brokers
	 * @param zk_host
	 * @return
	 */
	public List<String> getTopics(){
		
		 List<String> topics = null;
		 ZkUtils zkUtils = null;
		 ZkClient zkClient = null;
		 
		try{
			
			final ZkConnection zkConnection = new ZkConnection(zk_host);
	        final int sessionTimeoutMs = 10 * 1000;
	        final int connectionTimeoutMs = 20 * 1000;
	        zkClient = new ZkClient(zk_host, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);

	        zkUtils = new ZkUtils(zkClient, zkConnection, false);
	        topics = scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
	        
		}catch(Exception e){
			logger.error(e);
		}finally{
			if (zkClient != null) {
				zkClient.close();
			}
		}
		return topics;   
	}
	
	public static class Builder{
		
		private String zk_host;
		private String server_host;
		
		public Builder(String zk_host) {
			this.zk_host = zk_host;
		}
		
		public Builder(String server_host, String zk_host) {
			this.server_host = server_host;
			this.zk_host = zk_host;
		}
		
		public Manager build() {
			return new Manager(this);
		}
	}
	
	public static void main(String args[]) throws Exception{
		
		Manager m = new Manager.Builder("localhost:8080", "192.168.106.10:2181").build();
//		http://localhost:8080/topics?zk_host=192.168.106.10:2181
		String url = "http://" + m.server_host + "/topics?zk_host=" + m.zk_host;
		System.out.println(url);
		String url2 = "http://" + m.server_host + "/brokers?zk_host=" + m.zk_host;
		System.out.println(m.execute(url2));
	}
	
}




