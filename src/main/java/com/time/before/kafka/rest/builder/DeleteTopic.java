package com.time.before.kafka.rest.builder;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * 
 * @author zhuhuihua
 *
 */
public class DeleteTopic extends Action{
	
	private static final Logger logger = LogManager.getLogger(DeleteTopic.class.getSimpleName());
	
	protected DeleteTopic(Builder builder){
		this.topic_name = builder.topic_name;
		this.zk_host = builder.zk_host;
		this.server_host = builder.server_host;
	}
	
	public String getRestMethodName() {
        return "delete";
    }
	
	public String delete(String zk_host){
		
		ZkClient zkClient = null;
		ZkUtils zkUtils = null;
		
		try {

			int sessionTimeOutInMs = 15 * 1000; 
			int connectionTimeOutInMs = 10 * 1000; 

			zkClient = new ZkClient(zk_host, sessionTimeOutInMs, connectionTimeOutInMs,
					ZKStringSerializer$.MODULE$);
			zkUtils = new ZkUtils(zkClient, new ZkConnection(zk_host), false);
			
			 if (!AdminUtils.topicExists(zkUtils, topic_name)) {
				 return "topic is not exited !!!" + topic_name;
			 }else{
				 AdminUtils.deleteTopic(zkUtils, topic_name);
				 return "Deleted  " + topic_name;
			 }

		} catch (Exception e) {
			logger.error(e);
			return e.toString();

		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
	}
	
	public static class Builder{
		
	    private String topic_name;
	    private String zk_host;
	    private String server_host;
	    
	    public Builder(String topic_name) {
            this.topic_name = topic_name;
        }
	    
	    public Builder(String zk_host, String server_host, String topic_name) {
            this.topic_name = topic_name;
            this.zk_host = zk_host;
            this.server_host = server_host;
        }
	    
	    public DeleteTopic build(){
	    	return new DeleteTopic(this);
	    }
	}
	
	public static void main(String args[]) throws Exception{
		
//		http://localhost:8080/delete?zk_host=192.168.106.10:2181&topic_name=test
		DeleteTopic topic = new DeleteTopic.Builder("192.168.106.10:2181", "localhost:8080", "test").build();
		String url = "http://" + topic.server_host + "/" + topic.getRestMethodName() 
		+ "?zk_host=" + topic.zk_host + "&topic_name=" + topic.topic_name;
		
		System.out.println(url);
		System.out.println(topic.execute(url));
	}

}
