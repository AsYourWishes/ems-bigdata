package com.thtf.bigdata.test

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONArray
import java.util.regex.Pattern

object MyScalaTest {
  
  def main(args: Array[String]): Unit = {
    
//    jsonTest()
//    regexTest()
//    distinctTest()
//    stringCompare
//		 stringBuilderTest
//    codeFormatTest
    jsonCompareTest
    
    
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