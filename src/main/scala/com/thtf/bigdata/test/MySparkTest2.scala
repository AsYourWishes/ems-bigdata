package com.thtf.bigdata.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MySparkTest2 {
  
//  Logger.getLogger(this.getClass).setLevel(Level.ERROR) // 不生效
//  Logger.getLogger("org").setLevel(Level.ERROR) // 生效
  
  def main(args: Array[String]): Unit = {
    
//	  Logger.getLogger(this.getClass).setLevel(Level.ERROR) // 不生效
		  Logger.getLogger("org").setLevel(Level.ERROR) // 生效
    
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName))
    
    val arr = Array(1,2,3)
    
    sc.parallelize(arr).foreach(println)
    
    
    
  }
  
  
  
  
  
  
  
}