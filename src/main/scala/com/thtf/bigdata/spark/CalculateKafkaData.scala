package com.thtf.bigdata.spark

import org.slf4j.LoggerFactory
import com.thtf.bigdata.util.PropertiesUtils
import com.thtf.bigdata.common.PropertiesConstant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.thtf.bigdata.functions.ZkOrKafkaFunctions
import com.thtf.bigdata.kafka.ZkKafkaOffsetManager
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONArray
import com.thtf.bigdata.functions.PhoenixFunctions
import com.thtf.bigdata.spark.accumulator.JsonAccumulator
import com.alibaba.fastjson.JSON
import com.thtf.bigdata.functions.SparkFunctions

/**
 * 从kafka中拉取数据，进行各项要求的计算，并存储到对应的统计表中。
 */
object CalculateKafkaData {
  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(this.getClass)

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
    // 读取kafka并处理数据的group为：kafka.groupId.streamProcessing=calculateKafkaData-001
    val groupId = PropertiesUtils.getPropertiesByKey(PropertiesConstant.KAFKA_GROUPID_STREAMPROCESSING)
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
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))

    //--设置检查点目录
    ssc.checkpoint(checkpoint)

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
    val inputDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Seq(topicsSet.head), kafkaParams, offsetsMap))

    /*
     * 创建累加器
     */
    val sc = ssc.sparkContext
    // tbl_item_current_info
    val currentInfoAccu = new JsonAccumulator
    sc.register(currentInfoAccu)
    // data_access
    val dataAccessAccu = new JsonAccumulator
    sc.register(dataAccessAccu)
    /*
     * 处理从kafka接受的数据，并存入对应表中
     */
    inputDStream.foreachRDD(rdd => {
      
      val sc = rdd.sparkContext
      /*
  		 * 创建广播变量
  		 */
      // (code,(basic_code,id))     (01-1,(A,11))
      val bcItemType = sc.broadcast(PhoenixFunctions.getItemTypeTable())
      // 表的最大值
      val bcMaxValue = sc.broadcast(PhoenixFunctions.getItemMaxValue())
      // 虚拟设备列表
      val bcVirtualItem = sc.broadcast(PhoenixFunctions.getVirtualItemList())
      // ItemCode Map(id-itemCode)
      val bcItemCodeById = sc.broadcast(PhoenixFunctions.getItemCodeById())
      // tbl_subentry Map(itemCode-subentry code) 010、01A、01B
      val bcSubentryCode = sc.broadcast(PhoenixFunctions.getSubentryMap())
      // 获取tbl_item_current_info表
      val bcCurrentInfo = sc.broadcast()
      //    println(bcItemType.value.size)
      //    println(bcMaxValue.value.size)
      //    println(bcVirtualItem.value.size)
      //    println(bcItemCodeById.value.size)
      //    println(bcSubentryCode.value.size)

      // 获取当前小时、天、月的时间
      val allTime = SparkFunctions.getAllTime()
      /*
       * 处理batch数据
       */
      val resultRdd = rdd.mapPartitions(partIt => {
        val mapPartResult = ArrayBuffer[((String,String),JSONArray)]()
        while (partIt.hasNext) {
          val nextJsonArray = JSON.parseArray(partIt.next().value())
    		  try {
    		    val checkTimestamp = SparkFunctions.checkStringTime(nextJsonArray.getString(9))
		    		// 记录data_access数据，一个小时去重写入表一次
		    		if(checkTimestamp){
		    			val dataAccessJson = new JSONArray
    					dataAccessJson.add(nextJsonArray.getString(0))
    					dataAccessJson.add(nextJsonArray.getString(1))
    					dataAccessJson.add(nextJsonArray.getString(9))
    					// 计入data_access累加器
    					dataAccessAccu.add(dataAccessJson)
		    		}
            // 筛选符合计算要求的数据
            if (nextJsonArray.getString(0) != null && // BuildingID
              nextJsonArray.getString(1) != null && // GateID
              nextJsonArray.getString(2) != null && // MeterID
              nextJsonArray.getString(3) != null && // ParamID
              nextJsonArray.getString(6) != null && // Status
              SparkFunctions.checkStringNumber(nextJsonArray.getString(7)) && // Value
              checkTimestamp // Timestamp
              ) {
              // ((s"${nextJsonArray.getString(0)}_${nextJsonArray.getString(1)}_${nextJsonArray.getString(2)}", nextJsonArray.getString(3)),nextJsonArray)
              mapPartResult.append(((s"${nextJsonArray.getString(0)}_${nextJsonArray.getString(1)}_${nextJsonArray.getString(2)}", nextJsonArray.getString(3)),nextJsonArray))
            }
          } catch {
            case t: Throwable => t.printStackTrace()
            log.error(s"字符串数据转换JSONArray失败，失败数据为$nextJsonArray")
          }
        }
        mapPartResult.toIterator
      }).groupByKey()
      .mapPartitions(partIt => {
//        var currentInfoJson = 
        val mapPartResult = ArrayBuffer[JSONArray]()
        
        
        mapPartResult.toIterator
      })
      
      
      
      
      
      
      
      
      

      // 提交offset
      val newOffsetsMap = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetManager.saveOffsets(newOffsetsMap, groupId)
      log.info("更新offset成功,offset:")
      newOffsetsMap.foreach(offset => {
        log.info("partition:" + offset.partition + ",起始offset：" + offset.fromOffset + ",截至offset：" + offset.untilOffset)
      })
    })

    // 启动sparkstreaming
    // 保持sparkstreaming持续运行
    ssc.start()
    var isRunning = true
    while (isRunning) {
      if (ssc.awaitTerminationOrTimeout(10000)) {
        log.error("WARNING!!! Spark StreamingContext 已经意外停止！")
        // 停止scala程序
        isRunning = false
      }
      if (isRunning && offsetManager.readFlag(Seq(topicsSet.head), groupId)) {
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