package com.thtf.bigdata.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import com.thtf.bigdata.kafka.ZkKafkaOffsetManager
import com.thtf.bigdata.util.PropertiesUtils
import com.thtf.bigdata.common.PropertiesConstant
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.kafka.common.TopicPartition
import org.apache.spark.util.LongAccumulator

object MyKafkaTest {
  
  val zkUrl = PropertiesUtils.getPropertiesByKey(PropertiesConstant.ZOOKEEPER_URL)
  val groupId = "kafka-test-105"
  
  val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "nn:6667,snn:6667,dn0:6667,dn1:6667,dn2:6667",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      // 可以使用这个配置，latest自动重置偏移量为最新的偏移量,earliest
      "auto.offset.reset" -> "earliest",
      // 如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> "false",
      // 单次最大拉取量->不生效
      // "max.poll.records" -> "200",
      // 用于标识这个消费者属于哪个消费团体
      "group.id" -> groupId)
  
  val topic = "test-spark-001"
  
  val ssc = new StreamingContext(
      new SparkConf()
      .setMaster("local[2]")
      .set("spark.streaming.kafka.maxRatePerPartition", "1")
      .setAppName(this.getClass.getSimpleName),
      Duration(5000))
  
//  ssc.checkpoint("checkpoint")
  
//  val offsetRanges = new ZkKafkaOffsetManager(zkUrl).readOffsets(Set(topic).toSeq, groupId)
  val offsetRanges = Map(new TopicPartition(topic,1) -> 6000L)
  
  val inputDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set(topic), kafkaParams, offsetRanges))
  
  def main(args: Array[String]): Unit = {
    
    val accu = new LongAccumulator
    ssc.sparkContext.register(accu)
    
    inputDStream.foreachRDD(rdd => {
      rdd.foreach(consumerRecord => {
        println(consumerRecord)
      })
      accu.add(1)
      println(accu.value)
//      inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    
    ssc.start()
    var isRunning = true
    while (isRunning) {
      ssc.awaitTerminationOrTimeout(1000)
      if(accu.value == 3){
    	  ssc.stop(true, true)
    	  isRunning = false
      }
    }
//    ssc.awaitTermination()
    
  }
  
  def kafkaOffsetTest(){
	  
    
    
    
  }
  
  
  
  
}



