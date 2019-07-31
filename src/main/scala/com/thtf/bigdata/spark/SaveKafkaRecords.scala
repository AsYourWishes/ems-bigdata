package com.thtf.bigdata.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import com.thtf.bigdata.util.PropertiesUtils
import com.thtf.bigdata.common.PropertiesConstant
import org.apache.spark.SparkConf
import com.thtf.bigdata.functions.ZkOrKafkaFunctions
import com.thtf.bigdata.kafka.ZkKafkaOffsetManager
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies

/**
 * 读取kafka数据，筛选并存入DS_HisData历史数据表、或DS_HisData_error错误数据表中
 */
object SaveKafkaRecords {
  def main(args: Array[String]): Unit = {
    
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val log = Logger.getLogger(this.getClass)
    
    // spark配置
    val master = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_MASTER)
    val maxRatePerPart = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_MAXRATEPERPARTITION)
    val batchInterval = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_BATCH).toLong
    val checkpoint = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_CHECKPOINT)
    val saveErrorData = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_SAVEERRORDATA).toBoolean
    
    // kafka配置
    // <brokers> kafka的集群地址
    val brokers = PropertiesUtils.getPropertiesByKey(PropertiesConstant.KAFKA_BROKERS)
    // <topics> kafka的主题
    val topics = PropertiesUtils.getPropertiesByKey(PropertiesConstant.KAFKA_TOPICS)
    val groupId = PropertiesUtils.getPropertiesByKey(PropertiesConstant.KAFKA_GROUP_ID)
    val autoOffsetReset = PropertiesUtils.getPropertiesByKey(PropertiesConstant.KAFKA_AUTO_OFFSET_RESET)
    
    // zk配置
    val zkUrl = PropertiesUtils.getPropertiesByKey(PropertiesConstant.ZOOKEEPER_URL)
    
    var sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    // 测试用master和拉取量
    if (master != null && master != "") {
      sparkConf = sparkConf
        .setMaster(master)
        .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPart)
    }
    val ssc = new StreamingContext(sparkConf,Seconds(batchInterval))
    
    //--设置检查点目录
    ssc.checkpoint(PropertiesUtils.getPropertiesByKey(checkpoint))
    
    val topicsSet = topics.split(",").toSet
    
    // 获取topic的beginningOffsets
    val beginningOffsets = ZkOrKafkaFunctions.getBeginningOffsetsOfTopic(brokers, zkUrl, topicsSet.head)
    // 获取手动记录的offsets
    val offsetManager = new ZkKafkaOffsetManager(zkUrl)
    val recordedOffsets = offsetManager.readOffsets(Seq(topicsSet.head), groupId)
    // 比较并取得可用的offsets
    val offsetsMap = ZkOrKafkaFunctions.getViableOffsets(beginningOffsets, recordedOffsets)
    
    // 创建directStream
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      // 可以使用这个配置，latest自动重置偏移量为最新的偏移量,earliest
      "auto.offset.reset" -> autoOffsetReset,
      //如果是true，则这个消费者的偏移量会在后台自动提交,此处设为false，在处理完数据后再提交offset
      "enable.auto.commit" -> "false",
      // 用于标识这个消费者属于哪个消费团体
      "group.id" -> groupId)
    // 创建kafka输入流，消费kafka
    val inputDStream = KafkaUtils.createDirectStream(ssc, 
        LocationStrategies.PreferConsistent, 
        ConsumerStrategies.Subscribe[String,String](Seq(topicsSet.head), kafkaParams, offsetsMap))
    
    
    
    
    
    ssc.start()
    var isRunning = true
    while (isRunning) {
      if(ssc.awaitTerminationOrTimeout(1000)){
        log.error("WARNING!!! Spark StreamingContext 已经意外停止！")
        // 停止scala程序
        isRunning = false
      }
      if(isRunning && offsetManager.readFlag(Seq(topicsSet.head), groupId)){
        // Flag = true 则停掉任务
        ssc.stop(true, true)
        // 停止scala程序
        isRunning = false
        // Flag 改为 false
        offsetManager.saveFlag(Seq(topicsSet.head), groupId, false)
      }
    }
    
    
  }
}