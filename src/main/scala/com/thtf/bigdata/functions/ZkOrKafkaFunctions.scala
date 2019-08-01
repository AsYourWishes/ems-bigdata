package com.thtf.bigdata.functions

import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import kafka.utils.ZkUtils
import java.util.HashSet
import org.apache.kafka.common.TopicPartition

object ZkOrKafkaFunctions {

  /**
   * 获取topic的起始offsets
   */
  def getBeginningOffsetsOfTopic(brokers: String, zkUrl: String, topic: String) = {
    // 创建kafka消费者
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", brokers)
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](consumerProps)
    // 创建zkUtils
    val zkClientAndConn = ZkUtils.createZkClientAndConnection(zkUrl, 30000, 30000);
    val zkUtils = new ZkUtils(zkClientAndConn._1, zkClientAndConn._2, false)
    // 通过zkUtils获取topic的分区信息
    val partitions = new HashSet[TopicPartition]()
    zkUtils.getPartitionsForTopics(Seq(topic)).foreach(topics => {
      topics._2.foreach(ps => {
        partitions.add(new TopicPartition(topics._1, ps))
      })
    })
    // 通过consumer获取topics的beginningOffsets
    val javaMapIt = consumer.beginningOffsets(partitions).entrySet().iterator()
    // 将Java的Map转换为scala的Map
    val map = scala.collection.mutable.Map[TopicPartition, Long]()
    while (javaMapIt.hasNext()) {
      val entry = javaMapIt.next()
      map.put(entry.getKey, entry.getValue)
    }
    map.toMap
  }

  /**
   * 判断手动记录的offset是否有效，并返回有效的offset
   */
  def getViableOffsets(beginningOffsets: Map[TopicPartition, Long], recordedOffsets: Map[TopicPartition, Long]) = {
    // 如果手动记录为空，会按配置的earliest或latest消费
    // TODO 只有部分分区数据过期时，替换过期分区的offset
    var viableOffsets = recordedOffsets
    if (!recordedOffsets.isEmpty) {
      import scala.util.control.Breaks._
      breakable {
        for (begin <- beginningOffsets) {
          val recorded = recordedOffsets.getOrElse(begin._1, -1L)
          if (recorded > 0 && recorded < begin._2) {
            viableOffsets = beginningOffsets
            break
          }
        }
      }
    }
    viableOffsets
  }

}