package com.time.before.kafka.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
	
	private static final Logger logger = LogManager.getLogger(JsonUtil.class);
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	/**
	 * json to obj
	 * @param json
	 * @return
	 */
	public static Object Json2Object(String json){
		
		Object obj = null;
		
		try {
			obj =  mapper.readValue(json, Object.class);
		} catch (IOException e) {
			logger.error("Parse json failed.", e);
		}
		
		return obj;
	}
	
	/**
	 * json to map
	 * @param json
	 * @return
	 */
	public static Map<Object, Object> jsonToMap(String json){
		
		Map<Object, Object> map = new HashMap<Object, Object>();
		
		try {
			map = mapper.readValue(json, Map.class);
		} catch (IOException e) {
			logger.error("Json convert to map failed", map);
		}
		
		return map;
	}
	/**
	 * json to list
	 * @param json
	 * @return
	 */
	
	public static List<Object> jsonToList(String json){
		
		List<Object> list = null;
		try {
			list = mapper.readValue(json, List.class);
		} catch (IOException e) {
			logger.error("Json String convert to List failed", e);
		}
		
		return list;
	}
	
	/**
	 * obj to json 
	 * @param obj
	 * @return
	 */
	
	public static String Object2Json(Object obj){
		
		String value = null;
		
		try {
			value = mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			logger.error("Convert to json failed.", e);
		}
		
		return value;
	}
	
	public static void main(String args[]){
		
		Map<String, String> testMap = new HashMap<String, String>();
		testMap.put("aaa", "aaa");
		testMap.put("bbb", "bbb");
		testMap.put("ccc", "ccc");
		String json = JsonUtil.Object2Json(testMap);
		
		Map<String, String> map = (Map<String, String>) JsonUtil.Json2Object(json);
		System.out.println(map.get("aaa"));
		
	}

}
