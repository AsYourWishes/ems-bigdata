package com.thtf.bigdata.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;

public class JsonUtils {
	private static Comparator<JSONObject> c = (JSONObject a, JSONObject b) -> 
	(Integer.parseInt(a.getString("pointName").substring(2)) - 
			Integer.parseInt(b.getString("pointName").substring(2))); 
	public static JSONArray sortJsonArrayByDate(JSONArray jsonlist, String sortinfo) {
		ArrayList<JSONObject> list = new ArrayList<JSONObject> ();
	        JSONObject jsonObj = null;
	        for (int i = 0; i < jsonlist.size(); i++) {
	            jsonObj = (JSONObject)jsonlist.get(i);
	            list.add(jsonObj);
	        }
	        //排序操作
	        Collections.sort(list, c);        
	        //把数据放回去 
	        jsonlist.clear();
	        list.stream().forEach(e ->jsonlist.add(e));
		return jsonlist;
	}
	public static JSONObject StringToJSONObject(String source){
		return JSONObject.parseObject(source, Feature.OrderedField);
	}
}
