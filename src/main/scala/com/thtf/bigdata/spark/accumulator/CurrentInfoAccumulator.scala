package com.thtf.bigdata.spark.accumulator

import org.apache.spark.util.AccumulatorV2
import com.alibaba.fastjson.JSONArray
import scala.collection.mutable.Map
import com.thtf.bigdata.hbase.util.PhoenixHelper
import com.thtf.bigdata.functions.PhoenixFunctions
import com.thtf.bigdata.functions.SparkFunctions

class CurrentInfoAccumulator extends AccumulatorV2[JSONArray,Map[(String, String), JSONArray]] {
  
  private val jsonMap:Map[(String, String), JSONArray] = Map()
  
  def init(): Unit = {
    val resultArray = SparkFunctions.result2JsonArr(PhoenixHelper.query(PhoenixFunctions.INFO_NAMESPACE, PhoenixFunctions.current_info_table, null, null))
    for(json <- resultArray){
      jsonMap.put((json.getString(1),json.getString(5)), json)
    }
  }

  def add(v: JSONArray): Unit = {
    jsonMap.put((v.getString(1),v.getString(5)), v)
  }

  def copy(): AccumulatorV2[JSONArray, Map[(String, String), JSONArray]] = {
    ???
  }

  def isZero: Boolean = {
    jsonMap.isEmpty
  }

  def merge(other: AccumulatorV2[JSONArray, Map[(String, String), JSONArray]]): Unit = {
    ???
  }

  def reset(): Unit = {
    ???
  }

  def value: Map[(String, String), JSONArray] = {
    ???
  }
  
}