package com.time.before.kafka.rest.builder;

import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * 
 * @author zhuhuihua
 *
 */
public class CreateTopic extends Action {

	private static final Logger logger = LogManager.getLogger(CreateTopic.class.getSimpleName());

	protected CreateTopic(CreateBuilder builder) {
		this.zk_host = builder.zk_host;
		this.topic_name = builder.topic_name;
		this.server_host = builder.server_host;
		this.noOfReplication = builder.noOfReplication;
		this.noOfPartitions = builder.noOfPartitions;
	}

	public String getRestMethodName() {
		return "create";
	}
	
	/**
	 * 
	 * @param zookeeper_hosts zk hosts
	 * @param topic_name  
	 * @param noOfPartitions 分区个数 默认2
	 * @param noOfReplication 备份个数 默认3
	 * @return
	 */
	public String create(String zookeeper_hosts, int noOfPartitions, int noOfReplication) {

		ZkClient zkClient = null;
		ZkUtils zkUtils = null;
		
		if(noOfPartitions == 0){
			noOfPartitions = 2;
		}
		if(noOfReplication == 0){
			noOfReplication = 1;
		}

		try {

			int sessionTimeOutInMs = 15 * 1000; 
			int connectionTimeOutInMs = 10 * 1000; 

			zkClient = new ZkClient(zookeeper_hosts, sessionTimeOutInMs, connectionTimeOutInMs,
					ZKStringSerializer$.MODULE$);
			zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeper_hosts), false);
			Properties topicConfiguration = new Properties();
			
			
			 if (AdminUtils.topicExists(zkUtils, topic_name)) {
				 return "topic is exited !!!" + topic_name;
			 }else{
				 AdminUtils.createTopic(zkUtils, topic_name, noOfPartitions, noOfReplication, topicConfiguration,
							RackAwareMode.Enforced$.MODULE$);
			 }

		} catch (Exception e) {
			logger.error(e);
			return e.toString();

		} finally {

			if (zkClient != null) {
				zkClient.close();
			}
		}

		return "success create " + topic_name;
	}

	public static class CreateBuilder {

		public String topic_name;
		public String server_host;
		public String zk_host;
		public int noOfReplication;
		public int noOfPartitions;

		public CreateBuilder(String topic_name) {
			this.topic_name = topic_name;
		}
		
		public CreateBuilder(String server_host, String zk_host, String topic_name, int noOfPartitions, int noOfReplication) {
			this.server_host = server_host;
			this.zk_host = zk_host;
			this.topic_name = topic_name;
			this.noOfReplication = noOfReplication;
			this.noOfPartitions = noOfPartitions;
					
		}
		
		public CreateTopic build() {
			return new CreateTopic(this);
		}
	}
	
	public static void main(String args[]) throws Exception{
		
		CreateTopic topic = new CreateTopic.CreateBuilder("localhost:8080", "192.168.106.10:2181","test", 0, 0).build();
		String url = "http://" + topic.server_host + "/" + topic.getRestMethodName() + 
				"?topic_name=" + topic.topic_name + "&zk_host=" + topic.zk_host + 
				"&noOfReplication=" + topic.noOfReplication + "&noOfPartitions=" +topic.noOfPartitions;
		System.out.println("url is \t" + url);
		topic.execute(url);
	}

}
