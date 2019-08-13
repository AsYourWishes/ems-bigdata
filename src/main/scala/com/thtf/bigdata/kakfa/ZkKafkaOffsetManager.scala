package com.thtf.bigdata.kafka

import org.slf4j.LoggerFactory
import kafka.utils.ZkUtils
import org.apache.kafka.common.TopicPartition
import kafka.utils.ZKGroupTopicDirs
import org.apache.spark.streaming.kafka010.OffsetRange

object ZkKafkaOffsetManager {

}

class ZkKafkaOffsetManager(zkUrl: String) extends Serializable{
  //  private val logger = LoggerFactory.getLogger(classOf[ZkKafkaOffsetManager])
  private val logger = LoggerFactory.getLogger("org")

  private val zkClientAndConn = ZkUtils.createZkClientAndConnection(zkUrl, 30000, 30000);
  private val zkUtils = new ZkUtils(zkClientAndConn._1, zkClientAndConn._2, false)

  def readOffsets(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    val offsets = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionsForTopics = zkUtils.getPartitionsForTopics(topics)

    // /consumers/<groupId>/offsets/<topic>/<partition>
    partitionsForTopics.foreach(partitions => {
      val topic = partitions._1
      val groupTopicDirs = new ZKGroupTopicDirs(groupId, topic)

      partitions._2.foreach(partition => {
        val path = groupTopicDirs.consumerOffsetDir + "/" + partition
        try {
          val data = zkUtils.readData(path)
          if (data != null) {
            offsets.put(new TopicPartition(topic, partition), data._1.toLong)
            logger.info(
              "Read offset - topic={}, partition={}, offset={}, path={}",
              Seq[AnyRef](topic, partition.toString, data._1, path))
          }
        } catch {
          case ex: Exception =>
            // 如果没有读取到，按spark配置的方式读取
            // offsets.put(new TopicPartition(topic, partition), 0L)
            logger.info(
              "Read offset - not exist: {}, topic={}, partition={}, path={}",
              Seq[AnyRef](ex.getMessage, topic, partition.toString, path))
        }
      })
    })

    offsets.toMap
  }

  def saveOffsets(offsetRanges: Seq[OffsetRange], groupId: String): Unit = {
    offsetRanges.foreach(range => {
      val groupTopicDirs = new ZKGroupTopicDirs(groupId, range.topic)
      val path = groupTopicDirs.consumerOffsetDir + "/" + range.partition
      zkUtils.updatePersistentPath(path, range.untilOffset.toString)
      logger.info(
        "Save offset - topic={}, partition={}, offset={}, path={}",
        Seq[AnyRef](range.topic, range.partition.toString, range.untilOffset.toString, path))
    })
  }

  // 优雅的关闭sparkstreaming
  def readFlag(topics: Seq[String], groupId: String): Boolean = {
    val groupTopicDirs = new ZKGroupTopicDirs(groupId, topics(0))
    val path = groupTopicDirs.consumerOffsetDir + "/" + "flag"
    try {
      val data = zkUtils.readData(path)
      logger.info("The path of the flag on zookeeper is :" + path)
      data._1.toBoolean
    } catch {
      case ex: Exception =>
        saveFlag(topics, groupId, false)
        false
    }
  }

  def saveFlag(topics: Seq[String], groupId: String, flag: Boolean): Unit = {
    val groupTopicDirs = new ZKGroupTopicDirs(groupId, topics(0))
    val path = groupTopicDirs.consumerOffsetDir + "/" + "flag"
    zkUtils.updatePersistentPath(path, flag.toString)
    logger.info(
      "Save flag - topic={}, group={}, path={}, flag={}",
      Seq[AnyRef](topics(0), groupId, path, flag.toString()))
  }
  
  // 读取上次处理历史数据的时间记录
  def readBeginTime(topics: Seq[String], groupId: String): String = {
    val groupTopicDirs = new ZKGroupTopicDirs(groupId, topics(0))
    val path = groupTopicDirs.consumerOffsetDir + "/" + "time"
    try {
      val data = zkUtils.readData(path)
      logger.info("The path of the flag on zookeeper is :" + path)
      data._1
    } catch {
      case ex: Exception =>
        null
    }
  }
  // 记录本次处理历史数据的时间
  def saveBeginTime(topics: Seq[String], groupId: String, time: String): Unit = {
    val groupTopicDirs = new ZKGroupTopicDirs(groupId, topics(0))
    val path = groupTopicDirs.consumerOffsetDir + "/" + "time"
    zkUtils.updatePersistentPath(path, time)
    logger.info(
      "Save flag - topic={}, group={}, path={}, flag={}",
      Seq[AnyRef](topics(0), groupId, path, time))
  }
  
  
  
  
}