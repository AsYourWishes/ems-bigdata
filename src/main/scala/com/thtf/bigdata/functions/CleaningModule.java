package com.thtf.bigdata.functions;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.thtf.bigdata.util.JsonUtils;
import com.thtf.bigdata.hbase.util.PhoenixHelper;
import com.thtf.bigdata.util.PropertiesUtils;

public class CleaningModule{
	private static final Logger LOG = LoggerFactory.getLogger(CleaningModule.class);
	// private static final long serialVersionUID = 1L;
	
	public static Map tableInfo = new HashedMap();

	/**
     * 获取所有列数据类型
     * @param tableName:命名空间~表名
     * @return List<String>:列数据类型
     * @throws Exception
     */
	public static List<String> getColumnsType(String tableName) throws Exception {
        //3.表信息
        JSONObject tabinfo;
        //优先取缓存信息,其次到公共表中取,并写入缓存
        if (!tableInfo.containsKey(tableName)) {
            tabinfo = getTableInfoFromHbase(tableName);
        } else {
            tabinfo = (JSONObject) tableInfo.get(tableName);
        }
        if (tabinfo == null) {
        	LOG.error("没有查询到对应的表信息:{}",tableName);
            return null;
        }
        //3.1 列信息
        JSONArray columns = tabinfo.getJSONArray("columnstype");
        if (columns == null) {
        	LOG.error("没有查询到对应的列信息");
            return null;
        }
        return columns.toJavaList(String.class);
    }
    
    /**
     * 获取表结构信息
     * @param row：命名空间~表名
     * @return JSONObject:表结构
     */
    public static JSONObject getTableInfoFromHbase(String row) {
        try {
            //从配置文件中读取公共表信息
            JSONObject pubTableObj = JsonUtils.StringToJSONObject(PropertiesUtils.getPropertiesByKey("hbase.pubtable"));
            JSONObject colObj = pubTableObj.getJSONObject("columns");
            String pubTableName = pubTableObj.getString("tablename");
            String primayKey = pubTableObj.getString("primay");
            String primayKey_type = colObj.getString(primayKey);
            String columnName = pubTableObj.getString("colname");
            //where语句
            StringBuilder sb = new StringBuilder();
            sb.append("\"").append(primayKey).append("\"=");
            if ("varchar".equals(primayKey_type)) {
                sb.append("'").append(row).append("'");
            } else {
                sb.append(row);
            }
            String[] wheres = {sb.toString()};
            //用phoenix方式获取数据
            ResultSet rs = PhoenixHelper.query("", pubTableName, null, wheres);
            String tableStr = "";
            while (rs.next()) {
                tableStr = rs.getString(columnName);
            }
            JSONObject o = JsonUtils.StringToJSONObject(tableStr);
            tableInfo.put(row, o);
            return o;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
	//判断数据类型
	public static Boolean judgeType(Object data, String type){
		Boolean bResult = false;
		
		switch (type) {
        case "bigint":
        	if (data instanceof Long) {
        		bResult = true;
        	}
        	break;
        case "int":
            if (data instanceof Integer) {
        		bResult = true;
        	}
        	break;
        case "short":
        case "tiny":
            if (data instanceof Byte) {
        		bResult = true;
        	}
        	break;
        case "bit":
            if (data instanceof Boolean) {
        		bResult = true;
        	}
        	break;
        case "float":
        case "double":
        case "money":
        case "numeric":
        case "decimal":
        	if (data instanceof Float) {
        		bResult = true;
        	}
        	else if (data instanceof Double) {
        		bResult = true;
        	}
        	else if (data instanceof BigDecimal) {
        		bResult = true;
        	}
        	break;
        case "string":
        	bResult = true;
        	break;
        default:
        	break;
		}
		
		return bResult;
	}
}
