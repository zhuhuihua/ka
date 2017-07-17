package com.time.before.kafka.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.network.ListenerName;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.time.before.kafka.rest.builder.CreateTopic;
import com.time.before.kafka.rest.builder.DeleteTopic;
import com.time.before.kafka.rest.builder.Info;
import com.time.before.kafka.rest.builder.Manager;
import com.time.before.kafka.utils.JsonUtil;
import com.time.before.kakfa.producer.NotificationProducerThread;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;

@RestController
public class KafkaRestController {

	// http://localhost:8080/query?id=1&key=value&name=Kotori&age=17
	@RequestMapping(value = "/query", method = RequestMethod.GET)
	public void queryMethod(@RequestParam String id, @RequestParam Map<String, String> queryParameters,
			@RequestParam MultiValueMap<String, String> multiMap) {
		System.out.println("id=" + id);
		System.out.println(queryParameters);
		System.out.println(multiMap);
	}

	@RequestMapping(value = "/put", method = RequestMethod.GET)
	public void producer_data(@RequestParam String brokers ,@RequestParam String topic, @RequestParam String msg) {
		NotificationProducerThread producer = new  NotificationProducerThread(brokers, topic);
		producer.send(msg);
		
	}
	
	@RequestMapping(value = "/create", method = RequestMethod.GET)
	public String create(@RequestParam Map<String, String> params){
		
		new CreateTopic.CreateBuilder(params.get("topic_name")).build()
		.create(params.get("zk_host"), Integer.valueOf(params.get("noOfPartitions")), 
				Integer.valueOf(params.get("noOfReplication")));
		
		return "success";
	}
	
	@RequestMapping(value = "/delete", method = RequestMethod.GET)
	public String delete(@RequestParam Map<String, String> params){
		
		String msg = new DeleteTopic.Builder(params.get("topic_name")).build()
		.delete(params.get("zk_host"));
		
		return msg;
	}
	
	@RequestMapping(value = "/brokers", method = RequestMethod.GET)
	public String get_brokers(@RequestParam String zk_host){
		
		List<Info> infos = new ArrayList<Info>();
		
		List<Broker> brokers = new Manager.Builder(zk_host).build().getBrokers();
		BrokerEndPoint bep = null;
		for(Broker b: brokers){
			bep = b.getBrokerEndPoint(new ListenerName("PLAINTEXT"));
			Info info = new Info(bep.id(), bep.host(), bep.port());
			infos.add(info);
		}
		return JsonUtil.Object2Json(infos);
	}
	
	@RequestMapping(value = "/topics", method = RequestMethod.GET)
	public String get_topics(@RequestParam String zk_host){
		
		List<String> topics = new Manager.Builder(zk_host).build().getTopics();
		return JsonUtil.Object2Json(topics);
	}

	@RequestMapping(value = "/batch", method = RequestMethod.GET)
	public String batch(@RequestParam Map<String, String> queryParameters) {

		return null;
	}

}