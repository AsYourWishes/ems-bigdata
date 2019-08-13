package com.thtf.bigdata.test

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONArray
import java.util.regex.Pattern
import scala.math.BigDecimal
import com.thtf.bigdata.functions.SparkFunctions
import com.thtf.bigdata.spark.CalculateHisData
import com.thtf.bigdata.functions.PhoenixFunctions

object MyScalaTest {
  
  def main(args: Array[String]): Unit = {
    
//    jsonTest()
//    regexTest()
//    distinctTest()
//    stringCompare
//		 stringBuilderTest
//    codeFormatTest
//    jsonCompareTest
//    bigdecimalTest
//    stringNumberTest
//    numTest
//    timeTest
//    println(CalculateHisData.getyy_MM_ddTime(1523333333333L))
//    jsonTimeSort
//    stringNumberTest
    PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.elec_hour_table, "2019-07-17 02:00:00", null, null)
    
    
  }
  
  def jsonTimeSort(){
    val json1 = new JSONArray
    json1.add(3)
    json1.add("2019-07-14 11:00:00")
    val json2 = new JSONArray
    json2.add(2)
    json2.add("20190716110000")
    val json3 = new JSONArray
    json3.add(5)
    json3.add("20190616110000")
    
    val arr = Array(json1,json2,json3)
    
    arr.sortBy(json => CalculateHisData.getTimestamp(json.getString(1))).foreach(println)
    
    
    
    
    
  }
  
  
  def timeTest(){
    val alltime = SparkFunctions.getAllTime("20190717000000")
    println(alltime.currentMonthTime)
    println(alltime.nextMonthTime)
  }
  
  def numTest(){
    val a = "2723.87"
		val b = "2723.83"
		println(BigDecimal(a) - BigDecimal(b))
		
		val c = 0.02d
		val d = 0.06d
		println(SparkFunctions.getSum(d, c))
    
  }
  
  
  def stringNumberTest(){
    val str = "123,456"
//		println(str.toDouble)
    val json = new JSONArray
    json.add(str)
//    println(json.get(0))
    println(BigDecimal(json.get(0).toString()))
    
  }
  
  
  def bigdecimalTest(){
    try {
    	val a = BigDecimal("")
			println(a)
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    val json = new JSONArray
    json.add(BigDecimal("12345"))
    println(json)
    println(json.getString(0))
    json.add("123,456")
    println(json.getString(1))
    println(json.getDouble(1))
    println(json.getInteger(1))
    json.add(null)
    println(json.getDouble(2))
    println(json.getInteger(2))
    val dou = json.getDouble(2)
    println(dou == null)
    println(json.getDouble(2) == null)
    println(json.getInteger(2) == null)
    println(json.getString(2))
    
    
    
  }
  
  
  def jsonTest(){
    val arr = new ArrayBuffer[JSONArray]
	  val json = new JSONArray
	  json.add("1")
	  json.add("2")
	  arr.append(json)
	  arr.foreach(println)
	  
	  arr(0).add(0, "2")
	  arr.foreach(println)
  }
  
  def regexTest(){
    val pattern = Pattern.compile(" *A *")
    println(pattern.matcher("   A ").matches())
    println(pattern.matcher("A").matches())
    println(pattern.matcher(".A").matches())
    println(pattern.matcher("AA").matches())
    println(pattern.matcher("cAc").matches())
    
  }
  
  def distinctTest(){
    val arr = new ArrayBuffer[JSONArray]
    val json = new JSONArray
    json.add("1")
    json.add("2")
    json.add(3)
    arr.append(json)
    arr.append(json)
    val json2 = new JSONArray
    json2.add("1")
    json2.add("2")
    json2.add(4)
    arr.append(json2)
    arr.append(json2)
    val json3 = new JSONArray
    json3.add("1")
    json3.add("2")
    json3.add("4")
    arr.append(json3)
    arr.append(json3)
    arr.foreach(println)
    println("distinct")
    arr.distinct.foreach(println)
  }
  
  def stringCompare(){
    
    val a = "11"
    val b = "12"
    val c = " 11"
    
    println(a > b)
    println(a > c)
    println(a == c)
    println(b > a)
    println(b > c)
    println(b == c)
  }
  
  def stringBuilderTest(){
    val a = new StringBuilder
    a.append("1")
    a.append("2")
    println(a)
    println(a.append("3"))
    println(a)
    a.append(if (1 == 2) "4" else "456789")
    println(a)
  }
  
  def codeFormatTest(){
    val flag = {
      1 == 1 &&
      2 == 2 &&
      "3" == "3"
    }
    println(flag)
  }
  
  def jsonCompareTest(){
    val json1 = new JSONArray
    json1.add("123")
    json1.add(null)
    json1.add(123)
    val json2 = new JSONArray
    json2.add("123")
    json2.add(null)
    json2.add(123)
    println(json1 == json2)
    json2.add(123)
    println(json1 == json2)
    
    println("abc",123)
    println((s"${json1.getString(0)}_${json1.getString(1)}_${json1.getString(2)}",json1.getString(2)),(s"${json1.getString(0)}_${json1.getString(1)}_${json1.getString(2)}",json1.getString(2)))
    println((s"${json1.getString(0)}_${json1.getString(1)}_${json1.getString(2)}",json1.getString(2)) == (s"${json1.getString(0)}_${json1.getString(1)}_${json1.getString(2)}",json1.getString(2)))
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}