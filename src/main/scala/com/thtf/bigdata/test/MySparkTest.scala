package com.thtf.bigdata.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import com.alibaba.fastjson.JSONArray
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.ArrayBuffer
import com.thtf.bigdata.spark.accumulator.CurrentInfoAccumulator
import com.thtf.bigdata.functions.SparkFunctions

object MySparkTest {

  Logger.getLogger(this.getClass).setLevel(Level.ERROR)

  val sc = new SparkContext(
    new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.driver.allowMultipleContexts", "true"))

  Logger.getLogger(this.getClass).setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    mapPartitionsTest()

  }

  def accuTest() {
    val accu = new CurrentInfoAccumulator
    sc.register(accu)
    accu.init()
    accu.value.values.toArray.foreach(println)
  }

  def rddTest() {
    val time = 20190729170000L
    var alltime = SparkFunctions.getAllTime(time.toString())
    val arr = Array("1", "2")
    for (i <- 1 to 2) {
      println("------------" + i + "----------")
      sc.parallelize(arr, 2).foreachPartition(part => {
        while (part.hasNext) {
          println(part.next() + "~" + alltime.currentHourTime)
        }
      })
      alltime = SparkFunctions.getAllTime((time + 10000).toString())
    }
  }

  def mapPartitionsTest() {

    val arr = Array(("1",1), ("2",2), ("1",3), ("2",4))
    sc.parallelize(arr, 2).groupByKey()
    .mapPartitions(partIt => {
      val array = new ArrayBuffer[String]
      println(TaskContext.getPartitionId())
      while (partIt.hasNext) {
        println(partIt.next())
      }
      if(TaskContext.getPartitionId() == 1) array.append("1")
      array.toIterator
    }).collect().foreach(println)

  }

}