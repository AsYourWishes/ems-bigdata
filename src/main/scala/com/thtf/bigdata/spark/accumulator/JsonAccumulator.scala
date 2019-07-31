package com.thtf.bigdata.spark.accumulator

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONArray
import com.thtf.bigdata.functions.PhoenixFunctions

class JsonAccumulator extends AccumulatorV2[JSONArray,ArrayBuffer[JSONArray]] {
  
  private val jsonArray = new ArrayBuffer[JSONArray]

  def add(v: JSONArray): Unit = {
    jsonArray.append(v)
  }

  def copy(): AccumulatorV2[JSONArray, ArrayBuffer[JSONArray]] = {
    val newAccu = new JsonAccumulator
    jsonArray.synchronized{
      for(json <- jsonArray){
        newAccu.add(json)
      }
    }
    newAccu
  }

  def isZero: Boolean = {
    jsonArray.isEmpty
  }

  def merge(other: AccumulatorV2[JSONArray, ArrayBuffer[JSONArray]]): Unit = {
    other match {
      case o: JsonAccumulator => 
        for(json <- o.value){
          jsonArray.append(json)
        }
    }
  }

  def reset(): Unit = {
    jsonArray.clear()
  }

  def value: ArrayBuffer[JSONArray] = {
    jsonArray
  }
  
  
  
  
  
}