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
import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern
import com.thtf.bigdata.hbase.util.PhoenixHelper
import com.thtf.bigdata.functions.CleaningModule
import com.thtf.bigdata.util.FormulaUtil
import org.apache.spark.TaskContext

/**
 * 从kafka中拉取数据，进行各项要求的计算，并存储到对应的统计表中。
 */
object CalculateKafkaData {

  // yyyyMMddHHmmss -> yyyy-MM-dd HH:mm:ss
  def getyy_MM_ddTime(time: String) = {
    val yy_MM_dd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val yyMMdd = new SimpleDateFormat("yyyyMMddHHmmss")
    yy_MM_dd.format(yyMMdd.parse(time.replaceAll("\\D", "").dropRight(4) + "0000"))
  }
  def getyy_MM_ddTime(time: Long) = {
    val yy_MM_dd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    yy_MM_dd.format(new Date(time))
  }
  def getTimestamp(time: String) = {
    new SimpleDateFormat("yyyyMMddHHmmss").parse(time.replaceAll("\\D", "").dropRight(4) + "0000").getTime
  }

  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(this.getClass)

    // spark配置
    val master = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_MASTER)
    val maxRatePerPart = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_MAXRATEPERPARTITION)
    val batchInterval = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_BATCH).toLong
    val checkpoint = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_CHECKPOINT)
    val saveErrorData = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_SAVEERRORDATA).toBoolean
    
    val range = PropertiesUtils.getPropertiesByKey(PropertiesConstant.DATA_TIME_RANGE).toLong
    
    val dataAccessStorageLife = PropertiesUtils.getPropertiesByKey(PropertiesConstant.DATA_ACCESS_STORAGELIFE).toLong

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

    var sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 测试用master和拉取量
    if (master != null && master != "") {
      sparkConf = sparkConf
        .setMaster(master)
        .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPart)
    }
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))

    //--设置检查点目录
//    ssc.checkpoint(checkpoint)

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
    // 小时表数据
    //    val elecHourDataAccu = new JsonAccumulator
    //    sc.register(elecHourDataAccu,"elec_hour_data")
    //    val otherHourDataAccu = new JsonAccumulator
    //    sc.register(otherHourDataAccu,"other_hour_data")
    // 天表数据
    //    val elecDayDataAccu = new JsonAccumulator
    //    sc.register(elecDayDataAccu,"elec_day_data")
    //    val otherDayDataAccu = new JsonAccumulator
    //    sc.register(otherDayDataAccu,"other_day_data")
    //    // 月表数据
    //    val elecMonthDataAccu = new JsonAccumulator
    //    sc.register(elecMonthDataAccu,"elec_month_data")
    //    val otherMonthDataAccu = new JsonAccumulator
    //    sc.register(otherMonthDataAccu,"other_month_data")
    //    // 分项表数据
    //    val subHourDataAccu = new JsonAccumulator
    //    sc.register(subHourDataAccu,"sub_hour_data")
    //    val subDayDataAccu = new JsonAccumulator
    //    sc.register(subDayDataAccu,"sub_day_data")
    /*
     * 处理从kafka接受的数据，并存入对应表中
     */
    inputDStream.foreachRDD(rdd => {

      // 获取当前小时、天、月的时间
      val allTime = SparkFunctions.getAllTime()

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
      val bcCurrentInfo = sc.broadcast(PhoenixFunctions.getCurrentInfoMap())
      //    println(bcItemType.value.size)
      //    println(bcMaxValue.value.size)
      //    println(bcVirtualItem.value.size)
      //    println(bcItemCodeById.value.size)
      //    println(bcSubentryCode.value.size)
      /*
       * 处理batch数据
       */
      val hourResultRdd = rdd.mapPartitions(partIt => {
        val mapPartResult = ArrayBuffer[((String, String), JSONArray)]()
        while (partIt.hasNext) {
          val nextJsonArray = JSON.parseArray(partIt.next().value())
          try {
            val checkTimestamp = SparkFunctions.checkStringTime(nextJsonArray.getString(9),range)
            // 记录data_access数据，一个小时去重写入表一次
            if (checkTimestamp) {
              val dataAccessJson = new JSONArray
              dataAccessJson.add(nextJsonArray.getString(0))
              dataAccessJson.add(nextJsonArray.getString(1))
              dataAccessJson.add(nextJsonArray.getString(9).take(10))
              dataAccessJson.add("0")
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
              mapPartResult.append(((s"${nextJsonArray.getString(0)}_${nextJsonArray.getString(1)}_${nextJsonArray.getString(2)}", nextJsonArray.getString(3)), nextJsonArray))
            }
          } catch {
            case t: Throwable =>
              t.printStackTrace()
              log.error(s"字符串数据转换JSONArray失败，失败数据为$nextJsonArray")
          }
        }
        mapPartResult.toIterator
      }).groupByKey()
        .mapPartitions(partIt => {
          val mapPartResult = ArrayBuffer[JSONArray]()
          while (partIt.hasNext) {
            val keyTypeJsons = partIt.next()
            val itemName = keyTypeJsons._1._1
            val code = keyTypeJsons._1._2 // 01-1
            // 只计算tbl_item_type表中有的
            val hasItemType = bcItemType.value.getOrElseUpdate(code, null)
            if (hasItemType != null) {
              val typeCode = hasItemType._1 // A、B、C、null
              val typeId = hasItemType._2.toString() // 11、12、13、14、15
              val valueJsonArray = keyTypeJsons._2
                .toArray
                .filter(_.getString(6).trim() == "0") // 只计算status为0的数据
                .sortBy(json => getTimestamp(json.getString(9))) // 按时间升序排序
              if (!valueJsonArray.isEmpty) {
//                valueJsonArray.foreach(println)
                // 获取item_name上一次数据记录
                var lastCurrentInfo = bcCurrentInfo.value.getOrElse((itemName, typeId), null)
                // 获取item信息(maxValue和变比)
                var maxValueAndcoefficient = bcMaxValue.value.getOrElse((itemName, typeId), null)
                var maxValue: Double = 10000
                var coefficient: Double = 1
                if (maxValueAndcoefficient != null) {
                  maxValue = maxValueAndcoefficient._1
                  coefficient = maxValueAndcoefficient._2
                }
                var nextJson: JSONArray = null
                for (i <- 0 until valueJsonArray.length) {
                  nextJson = valueJsonArray(i)
                  val nextJsonTime = getyy_MM_ddTime(nextJson.getString(9))
                  //第一次获取设备值  不需要计算只存储
                  if (lastCurrentInfo == null) {
                    lastCurrentInfo = (nextJsonTime, SparkFunctions.getProduct(nextJson.getString(7), coefficient.toString()))
                  } else {
                    val nextTimestamp = getTimestamp(nextJsonTime)
                    val InfoTimestamp = getTimestamp(lastCurrentInfo._1)
                    //判断两个时间是否为同一小时 是则不操作
                    if (nextTimestamp - InfoTimestamp >= 3600000) {
                      //判断两帧数据时间间隔相差是否大于一小时，大于一小时说明数据断了，不计算，替换最新值
                      if (nextTimestamp - InfoTimestamp == 3600000) {
                        // 如果相差一小时，计算小时数据，并记录最新currentInfo
                        // 记算小时数据
                        val currentInfoValue = lastCurrentInfo._2
                        val nextValue = SparkFunctions.getProduct(nextJson.getString(7), coefficient.toString())
                        var energyValue = SparkFunctions.getSubtraction(currentInfoValue, nextValue)
                        var state = if (energyValue < maxValue) "0" else "2"
                        if (energyValue < 0) {
                          energyValue = 0
                          state = "2"
                        }
                        val totalRate = "0"
                        // 记录小时数据
                        val hourResultJson = new JSONArray
                        hourResultJson.add(itemName)
                        hourResultJson.add(getyy_MM_ddTime(lastCurrentInfo._1)) // 本次记录 - 上次记录 = 上小时数据
                        hourResultJson.add(energyValue)
                        hourResultJson.add(nextValue)
                        hourResultJson.add(totalRate)
                        hourResultJson.add(state)
                        hourResultJson.add(typeCode)
                        // 添加到mapPartition中
                        mapPartResult.append(hourResultJson)
                      }
                      lastCurrentInfo = (nextJsonTime, SparkFunctions.getProduct(nextJson.getString(7), coefficient.toString()))
                    }
                  }
                }
                val currentInfoJson = new JSONArray
                currentInfoJson.add(itemName)
                currentInfoJson.add(lastCurrentInfo._1)
                currentInfoJson.add(lastCurrentInfo._2)
                currentInfoJson.add(nextJson.getString(6))
                currentInfoJson.add(typeId)
                // 添加current_info数据到累加器中
                currentInfoAccu.add(currentInfoJson)
              }
            }
          }
          mapPartResult.toIterator
        }).cache()

      val hourResultArray = hourResultRdd.collect()

      // 写入计算好的小时数据
      val pattern = Pattern.compile(" *A *")
      // 电
      PhoenixFunctions.phoenixWriteHbase(
        PhoenixFunctions.DATA_NAMESPACE,
        PhoenixFunctions.elec_hour_table,
        hourResultArray.filter(json => {
          pattern.matcher(json.getString(6)).matches()
        }))
      log.info("写入电小时数据")
      //        hourDataResult.filter(json => { pattern.matcher(json.getString(6)).matches() }).foreach(println)
      // other
      PhoenixFunctions.phoenixWriteHbase(
        PhoenixFunctions.DATA_NAMESPACE,
        PhoenixFunctions.other_hour_table,
        hourResultArray.filter(json => {
          !pattern.matcher(json.getString(6)).matches()
        }))
      log.info("写入other小时数据")

      // 写入data_access表
      PhoenixFunctions.insertDataAccess(dataAccessAccu.value)
      log.info("更新data_access表")
      // 清空累加器
      dataAccessAccu.reset()
      
      // 更新tbl_item_current_info表
      PhoenixFunctions.updateCurrentInfo(currentInfoAccu.value)
      log.info("更新tbl_item_current_info表")
      // 清空累加器
      currentInfoAccu.reset()

      // 计算电分项数据
      hourResultRdd.groupBy(_.getString(0).split("_").head).foreachPartition(partIt => {
    	  var numSub = 0
        while (partIt.hasNext) {
          val keyValues = partIt.next()
          val buildingCode = keyValues._1
          val valueJsonArray = keyValues._2
            .toArray
            .distinct
            .filter(json => json.getString(5).trim() == "0" && Pattern.compile(" *A *").matcher(json.getString(6)).matches())
            .sortBy(json => getTimestamp(json.getString(1)))
          if (!valueJsonArray.isEmpty) {
            val connection = PhoenixHelper.getConnection(PhoenixFunctions.DATA_NAMESPACE)
            valueJsonArray.foreach(json => {
              val itemName = json.getString(0)
              var subCode = bcSubentryCode.value.getOrElse(itemName, null)
              if (subCode != null) {
                subCode = subCode.trim().take(3)
                val jsonTime = json.getString(1)
                val dataValue = json.getDouble(2)
                val time = SparkFunctions.getAllTime(jsonTime)
                // 查询分项小时表当前小时数据
                val lastSubHourArray = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.subentry_hour_table, jsonTime, null, buildingCode)
                var currentSubHourJson: JSONArray = null
                if (lastSubHourArray.isEmpty) {
                  currentSubHourJson = new JSONArray
                  currentSubHourJson.add(buildingCode)
                  currentSubHourJson.add(0d)
                  currentSubHourJson.add(0d)
                  currentSubHourJson.add(0d)
                  currentSubHourJson.add(0d)
                  currentSubHourJson.add(0d)
                  currentSubHourJson.add(0d)
                  currentSubHourJson.add(jsonTime)
                  if (subCode == "010") currentSubHourJson.set(1, dataValue)
                  if (subCode == "01A") currentSubHourJson.set(2, dataValue)
                  if (subCode == "01B") currentSubHourJson.set(3, dataValue)
                  if (subCode == "01C") currentSubHourJson.set(4, dataValue)
                  if (subCode == "01D") currentSubHourJson.set(5, dataValue)
                } else {
                  currentSubHourJson = lastSubHourArray.head
                  currentSubHourJson.set(1, currentSubHourJson.getDouble(1))
                  currentSubHourJson.set(2, currentSubHourJson.getDouble(2))
                  currentSubHourJson.set(3, currentSubHourJson.getDouble(3))
                  currentSubHourJson.set(4, currentSubHourJson.getDouble(4))
                  currentSubHourJson.set(5, currentSubHourJson.getDouble(5))
                  if (subCode == "010") currentSubHourJson.set(1, SparkFunctions.getSum(currentSubHourJson.getDouble(1), dataValue))
                  if (subCode == "01A") currentSubHourJson.set(2, SparkFunctions.getSum(currentSubHourJson.getDouble(2), dataValue))
                  if (subCode == "01B") currentSubHourJson.set(3, SparkFunctions.getSum(currentSubHourJson.getDouble(3), dataValue))
                  if (subCode == "01C") currentSubHourJson.set(4, SparkFunctions.getSum(currentSubHourJson.getDouble(4), dataValue))
                  if (subCode == "01D") currentSubHourJson.set(5, SparkFunctions.getSum(currentSubHourJson.getDouble(5), dataValue))
                }
                // 查询分项天表当天数据
                val lastSubDayArray = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.subentry_day_table, time.currentDayTime, null, buildingCode)
                var currentSubDayJson: JSONArray = null
                if (lastSubDayArray.isEmpty) {
                  currentSubDayJson = new JSONArray
                  currentSubDayJson.add(buildingCode)
                  currentSubDayJson.add(0d)
                  currentSubDayJson.add(0d)
                  currentSubDayJson.add(0d)
                  currentSubDayJson.add(0d)
                  currentSubDayJson.add(0d)
                  currentSubDayJson.add(0d)
                  currentSubDayJson.add(time.currentDayTime)
                  if (subCode == "010") currentSubDayJson.set(1, dataValue)
                  if (subCode == "01A") currentSubDayJson.set(2, dataValue)
                  if (subCode == "01B") currentSubDayJson.set(3, dataValue)
                  if (subCode == "01C") currentSubDayJson.set(4, dataValue)
                  if (subCode == "01D") currentSubDayJson.set(5, dataValue)
                } else {
                  currentSubDayJson = lastSubDayArray.head
                  currentSubDayJson.set(1, currentSubDayJson.getDouble(1))
                  currentSubDayJson.set(2, currentSubDayJson.getDouble(2))
                  currentSubDayJson.set(3, currentSubDayJson.getDouble(3))
                  currentSubDayJson.set(4, currentSubDayJson.getDouble(4))
                  currentSubDayJson.set(5, currentSubDayJson.getDouble(5))
                  if (subCode == "010") currentSubDayJson.set(1, SparkFunctions.getSum(currentSubDayJson.getDouble(1), dataValue))
                  if (subCode == "01A") currentSubDayJson.set(2, SparkFunctions.getSum(currentSubDayJson.getDouble(2), dataValue))
                  if (subCode == "01B") currentSubDayJson.set(3, SparkFunctions.getSum(currentSubDayJson.getDouble(3), dataValue))
                  if (subCode == "01C") currentSubDayJson.set(4, SparkFunctions.getSum(currentSubDayJson.getDouble(4), dataValue))
                  if (subCode == "01D") currentSubDayJson.set(5, SparkFunctions.getSum(currentSubDayJson.getDouble(5), dataValue))
                }
                // 更新分项小时表数据
                try {
                  PhoenixHelper.upsert(
                    connection,
                    PhoenixFunctions.subentry_hour_table,
                    currentSubHourJson,
                    CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + PhoenixFunctions.subentry_hour_table))
                    log.info(s"Partition-${TaskContext.getPartitionId()}:更新分项数据到小时表成功，数据为${currentSubHourJson}")
                } catch {
                  case t: Throwable =>
                    t.printStackTrace() // TODO: handle error
                    log.error(s"写入表${PhoenixFunctions.subentry_hour_table}失败！出错数据为：$currentSubHourJson");
                }
                // 更新分项天表数据
                try {
                  PhoenixHelper.upsert(
                    connection,
                    PhoenixFunctions.subentry_day_table,
                    currentSubDayJson,
                    CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + PhoenixFunctions.subentry_day_table))
                    log.info(s"Partition-${TaskContext.getPartitionId()}:更新分项数据到天表成功，数据为${currentSubDayJson}")
                } catch {
                  case t: Throwable =>
                    t.printStackTrace() // TODO: handle error
                    log.error(s"写入表${PhoenixFunctions.subentry_day_table}失败！出错数据为：$currentSubDayJson");
                }
                numSub = numSub + 1
              }
            })
          	try {
          		if(connection != null) connection.close()
          	} catch {
          	case t: Throwable => t.printStackTrace() // TODO: handle error
          	}
          }
        }
    	  log.info(s"Partition-${TaskContext.getPartitionId()}:更新分项表数据成功，更新数据${numSub}条")
      })
      log.info("更新分项表完成")
      // 计算对应的天表和月表数据
      hourResultRdd.groupBy(_.getString(0)).foreachPartition(partIt => {
    	  var numDayAndMonth = 0
        while (partIt.hasNext) {
          val keyValues = partIt.next()
          val itemName = keyValues._1
          val valueJsonArray = keyValues._2
            .toArray
            .distinct
            .filter(_.getString(5).trim() == "0")
            .sortBy(json => getTimestamp(json.getString(1)))
          if (!valueJsonArray.isEmpty) {
            val connection = PhoenixHelper.getConnection(PhoenixFunctions.DATA_NAMESPACE)
            valueJsonArray.foreach(json => {
              log.info(s"需要计算天表和月表的小时数据为：${json}")
              val jsonTime = json.getString(1)
              val typeCode = json.getString(6)
              val dataValue = json.getDouble(2)
              val realValue = json.getDouble(3)
              val rate = json.getDouble(4)
              val time = SparkFunctions.getAllTime(jsonTime)
              val matchesElec = Pattern.compile(" *A *").matcher(typeCode).matches()
              var hourTable = PhoenixFunctions.elec_hour_table
              var dayTable = PhoenixFunctions.elec_day_table
              var monthTable = PhoenixFunctions.elec_month_table
              if (!matchesElec) {
                hourTable = PhoenixFunctions.other_hour_table
                dayTable = PhoenixFunctions.other_day_table
                monthTable = PhoenixFunctions.other_month_table
              }
              // 计算天表数据
              // 获取天表当前时间的数据
              val lastDayData = PhoenixFunctions.getEnergyDataByTime(dayTable, time.currentDayTime, null, itemName)
              var currentDayData: JSONArray = null
              if (lastDayData.isEmpty) {
                currentDayData = new JSONArray
                currentDayData.add(itemName)
                currentDayData.add(time.currentDayTime)
                currentDayData.add(dataValue)
                currentDayData.add(realValue)
                currentDayData.add(rate)
                currentDayData.add("0")
                currentDayData.add(0d)
                currentDayData.add(0d)
                currentDayData.add(typeCode)
              } else {
                currentDayData = lastDayData.head
                currentDayData.set(2, SparkFunctions.getSum(currentDayData.getDouble(2), dataValue))
                currentDayData.set(3, realValue)
                currentDayData.set(4, SparkFunctions.getSum(currentDayData.getDouble(4), rate))
              }
              // 计算月表数据
              // 获取月表当前时间的数据
              val lastMonthData = PhoenixFunctions.getEnergyDataByTime(monthTable, time.currentMonthTime, null, itemName)
              var currentMonthData: JSONArray = null
              if (lastMonthData.isEmpty) {
                log.info("查询月表上次记录为空")
                currentMonthData = new JSONArray
                currentMonthData.add(itemName)
                currentMonthData.add(time.currentMonthTime)
                currentMonthData.add(dataValue)
                currentMonthData.add(realValue)
                currentMonthData.add(rate)
                currentMonthData.add("0")
                currentMonthData.add(typeCode)
              } else {
                log.info(s"查询月表上次记录为：${lastMonthData.head}")
                currentMonthData = lastMonthData.head
                currentMonthData.set(2, SparkFunctions.getSum(currentMonthData.getDouble(2), dataValue))
                currentMonthData.set(3, realValue)
                currentMonthData.set(4, SparkFunctions.getSum(currentMonthData.getDouble(4), rate))
              }
              // 更新天表数据
              try {
                PhoenixHelper.upsert(
                  connection,
                  dayTable,
                  currentDayData,
                  CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + dayTable))
                  log.info(s"Partition-${TaskContext.getPartitionId()}:更新天表成功，数据为${currentDayData}")
              } catch {
                case t: Throwable =>
                  t.printStackTrace() // TODO: handle error
                  log.error(s"写入表${dayTable}失败！出错数据为：$currentDayData");
              }
              // 更新月表数据
              try {
                PhoenixHelper.upsert(
                  connection,
                  monthTable,
                  currentMonthData,
                  CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + monthTable))
                  log.info(s"Partition-${TaskContext.getPartitionId()}:更新月表成功，数据为${currentMonthData}")
              } catch {
                case t: Throwable =>
                  t.printStackTrace() // TODO: handle error
                  log.error(s"写入表${monthTable}失败！出错数据为：$currentMonthData");
              }
              numDayAndMonth = numDayAndMonth + 1
            })
            try {
          		if(connection != null) connection.close()
          	} catch {
          	case t: Throwable => t.printStackTrace() // TODO: handle error
          	}
          }
        }
    	  log.info(s"Partition-${TaskContext.getPartitionId()}:更新天表和月表成功，更新数据${numDayAndMonth}条")
      })
      log.info("更新天表和月表完成")

      /*// 更新tbl_item_current_info表
      PhoenixFunctions.updateCurrentInfo(currentInfoAccu.value)
      log.info("更新tbl_item_current_info表")
      // 清空累加器
      currentInfoAccu.reset()*/

      // 当时间为整小时时，计算第三方传入的小时数据到分项数据
      // 当时间为整小时时，计算虚拟表数据
      if (allTime.currentMinuteTime == allTime.currentHourTime) {
        // 计算第三方插入数据的分项数据
        // 获取data_access表中type为2的数据
        val subDataAccess = PhoenixFunctions.getDataAccessList(allTime.currentHourTime, "2")
        log.info(s"计算第三方插入数据本次查询data_access时间为：${allTime.currentHourTime},查询数据量为${subDataAccess.length}")
        // 更新data_access表中type为2的数据到type为3
        PhoenixFunctions.updateDataAccess(allTime.currentHourTime, "2", "3")
        log.info(s"更新data_access表中${allTime.currentHourTime}之前的type=2的数据为type=3")
        // 删除data_access表中type为2的数据
        PhoenixFunctions.deleteDataAccess(allTime.currentHourTime, "2")
        log.info(s"删除data_access表中type为2,${allTime.currentHourTime}之前的数据")
        
        val deleteTime = getyy_MM_ddTime(getTimestamp(allTime.currentHourTime) - (dataAccessStorageLife * 1000*60*60*24*30))
        // 删除指定时间，data_access表中type为3的数据
        PhoenixFunctions.deleteDataAccess(deleteTime, "3")
        log.info(s"删除data_access表中type为3,${deleteTime}之前的数据")
        
        sc.parallelize(subDataAccess)
          .filter(json => SparkFunctions.checkStringTime(json.getString(2)))
          .groupBy(json => json.getString(0))
          .foreachPartition(partIt => {
            var numThirdSub = 0
            while (partIt.hasNext) {
              val keyValues = partIt.next()
              val buildingCode = keyValues._1
              val valueJsonArray = keyValues._2.toArray.distinct.sortBy(json => getTimestamp(json.getString(2) + "0000"))
              valueJsonArray.foreach(json => {
                val subTime = SparkFunctions.getAllTime(json.getString(2) + "0000")
                val subHourTime = subTime.currentHourTime
                val subDayTime = subTime.currentDayTime
                val collectorCode = json.getString(1)
                // 根据记录中的building_code和collector_code以及时间，获取小时表中对应数据
                val codeHourData = PhoenixFunctions.getSubHourData(buildingCode + "_" + collectorCode, subHourTime)
                if (!codeHourData.isEmpty) {
                  val connection = PhoenixHelper.getConnection(PhoenixFunctions.DATA_NAMESPACE)
                  var value = 0d
                  var value_a = 0d
                  var value_b = 0d
                  var value_c = 0d
                  var value_d = 0d
                  var rate = 0d
                  codeHourData.foreach(hourJson => {
                    val itemName = hourJson.getString(0)
                    var subCode = bcSubentryCode.value.getOrElse(itemName, null)
                    if (subCode != null) {
                      subCode = subCode.trim().take(3)
                      val jsonTime = hourJson.getString(1)
                      val dataValue = hourJson.getDouble(2)
                      // 只计算 value < 10w 的数据
                      if(dataValue < 100000){
                    	  if (subCode == "010") value = SparkFunctions.getSum(value, dataValue)
                			  if (subCode == "01A") value_a = SparkFunctions.getSum(value_a, dataValue)
                			  if (subCode == "01B") value_b = SparkFunctions.getSum(value_b, dataValue)
                			  if (subCode == "01C") value_c = SparkFunctions.getSum(value_c, dataValue)
                			  if (subCode == "01D") value_d = SparkFunctions.getSum(value_d, dataValue)
                      }
                    }
                  })
                  // 查询分项小时表当前小时数据
                  val lastSubHourArray = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.subentry_hour_table, subHourTime, null, buildingCode)
                  var currentSubHourJson: JSONArray = null
                  if (lastSubHourArray.isEmpty) {
                    currentSubHourJson = new JSONArray
                    currentSubHourJson.add(buildingCode)
                    currentSubHourJson.add(value)
                    currentSubHourJson.add(value_a)
                    currentSubHourJson.add(value_b)
                    currentSubHourJson.add(value_c)
                    currentSubHourJson.add(value_d)
                    currentSubHourJson.add(rate)
                    currentSubHourJson.add(subHourTime)
                  } else {
                    currentSubHourJson = lastSubHourArray.head
                    currentSubHourJson.set(1, SparkFunctions.getSum(currentSubHourJson.getDouble(1), value))
                    currentSubHourJson.set(2, SparkFunctions.getSum(currentSubHourJson.getDouble(2), value_a))
                    currentSubHourJson.set(3, SparkFunctions.getSum(currentSubHourJson.getDouble(3), value_b))
                    currentSubHourJson.set(4, SparkFunctions.getSum(currentSubHourJson.getDouble(4), value_c))
                    currentSubHourJson.set(5, SparkFunctions.getSum(currentSubHourJson.getDouble(5), value_d))
                  }
                  // 查询分项天表当天数据
                  val lastSubDayArray = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.subentry_day_table, subDayTime, null, buildingCode)
                  var currentSubDayJson: JSONArray = null
                  if (lastSubDayArray.isEmpty) {
                    currentSubDayJson = new JSONArray
                    currentSubDayJson.add(buildingCode)
                    currentSubDayJson.add(value)
                    currentSubDayJson.add(value_a)
                    currentSubDayJson.add(value_b)
                    currentSubDayJson.add(value_c)
                    currentSubDayJson.add(value_d)
                    currentSubDayJson.add(rate)
                    currentSubDayJson.add(subDayTime)
                  } else {
                    currentSubDayJson = lastSubDayArray.head
                    currentSubDayJson.set(1, SparkFunctions.getSum(currentSubDayJson.getDouble(1), value))
                    currentSubDayJson.set(2, SparkFunctions.getSum(currentSubDayJson.getDouble(2), value_a))
                    currentSubDayJson.set(3, SparkFunctions.getSum(currentSubDayJson.getDouble(3), value_b))
                    currentSubDayJson.set(4, SparkFunctions.getSum(currentSubDayJson.getDouble(4), value_c))
                    currentSubDayJson.set(5, SparkFunctions.getSum(currentSubDayJson.getDouble(5), value_d))
                  }
                  // 更新分项小时表数据
                  try {
                    PhoenixHelper.upsert(
                      connection,
                      PhoenixFunctions.subentry_hour_table,
                      currentSubHourJson,
                      CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + PhoenixFunctions.subentry_hour_table))
                    log.info(s"Partition-${TaskContext.getPartitionId()}:更新第三方分项小时表数据成功，数据为${currentSubHourJson}")
                  } catch {
                    case t: Throwable =>
                      t.printStackTrace() // TODO: handle error
                      log.error(s"写入表${PhoenixFunctions.subentry_hour_table}失败！出错数据为：$currentSubHourJson");
                  }
                  // 更新分项天表数据
                  try {
                    PhoenixHelper.upsert(
                      connection,
                      PhoenixFunctions.subentry_day_table,
                      currentSubDayJson,
                      CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + PhoenixFunctions.subentry_day_table))
                    log.info(s"Partition-${TaskContext.getPartitionId()}:更新第三方分项数据到天表成功，数据为${currentSubDayJson}")
                  } catch {
                    case t: Throwable =>
                      t.printStackTrace() // TODO: handle error
                      log.error(s"写入表${PhoenixFunctions.subentry_day_table}失败！出错数据为：$currentSubDayJson");
                  }
                 numThirdSub = numThirdSub + 1
                 try {
              		if(connection != null) connection.close()
              	 } catch {
              	 case t: Throwable => t.printStackTrace() // TODO: handle error
              	 }
                }
              })
            }
            log.info(s"Partition-${TaskContext.getPartitionId()}:更新第三方分项数据到小时表和天表成功，更新数据${numThirdSub}条")
          })
        log.info("计算第三方插入数据到分项表完成")
        

        // 计算虚拟表
        // 获取data_access表中type为0的数据
        // 计算虚拟表数据
        // 两个小时之前
        // 获取前一个小时有更新的data_access
        val dataAccessArray = PhoenixFunctions.getDataAccessList(allTime.lastHourTime)
        log.info(s"计算计算虚拟表本次查询data_access时间为：${allTime.lastHourTime},查询数据量为${dataAccessArray.length}")
        //        println("查询到的data_access表信息")
        //        dataAccessArray.foreach(println)
        val virtualHourData = sc.parallelize(dataAccessArray).filter(json => {
          SparkFunctions.checkStringTime(json.getString(2))
        }).foreachPartition(partIt => {
          val connection = PhoenixHelper.getConnection(PhoenixFunctions.DATA_NAMESPACE)
          while (partIt.hasNext) {
            val dataAccessJson = partIt.next()
            val buildingId = dataAccessJson.getString(0)
            // data_access中记录的时间
            var time = SparkFunctions.getAllTime(dataAccessJson.getString(2).trim() + "0000")
            // 获取记录时间的前一个小时
            val beforhour = time.lastHourTime
            time = SparkFunctions.getAllTime(beforhour)
            // 获取虚拟设备列表
            val virtualItemList = bcVirtualItem.value.filter(json => {
              json.getString(0) == buildingId
            })
            /*
            // 测试-----插入虚拟设备
            val testJson = new JSONArray
            testJson.add("430100A001")
            testJson.add("01")
            testJson.add("8888")
            testJson.add("8499 + 8500")
            testJson.add("10000")
            testJson.add("01-1")
            testJson.add("")
            testJson.add("A")
            virtualItemList.append(testJson)
            // 测试-----插入虚拟设备
            */
            virtualItemList.foreach(itemJson => {
              val typeCode = itemJson.getString(5)
              val hourTableName = if (typeCode == "01-1") PhoenixFunctions.elec_hour_table else PhoenixFunctions.other_hour_table
              val dayTableName = if (typeCode == "01-1") PhoenixFunctions.elec_day_table else PhoenixFunctions.other_day_table
              val monthTableName = if (typeCode == "01-1") PhoenixFunctions.elec_month_table else PhoenixFunctions.other_month_table
              var virtualLogic = itemJson.getString(3)
              val ids = new ArrayBuffer[String]
              if (virtualLogic != null) {
                virtualLogic = virtualLogic.replaceAll("-", "#")
                val itemIds = virtualLogic.split(" ")
                for (id <- itemIds) {
                  if (id.trim() != "") {
                    ids.append(id)
                  }
                }
              }
              val value = new StringBuilder
              val realValue = new StringBuilder
              val rate = new StringBuilder
              for (id <- ids) {
                if (Pattern.compile("[0-9]*").matcher(id).matches()) {
                  // 获取设备编码
                  val itemCode = bcItemCodeById.value.getOrElse(id.toLong, null)
                  if (itemCode != null) {
                    // 获取对应小时数据
                    val valueMap = PhoenixFunctions.getHourDataByCode(hourTableName, itemCode, beforhour, "0")
                    if (!valueMap.isEmpty) {
                      val mapValue = valueMap.head.getDouble(0)
                      val mapRealValue = valueMap.head.getDouble(1)
                      val mapRate = valueMap.head.getDouble(2)
                      value.append(if (mapValue == null) "0" else mapValue)
                      realValue.append(if (mapRealValue == null) "0" else mapRealValue)
                      rate.append(if (mapRate == null) "0" else mapRate)
                    } else {
                      value.append("0")
                      realValue.append("0")
                      rate.append("0")
                    }
                  } else {
                    value.append("0")
                    realValue.append("0")
                    rate.append("0")
                  }
                } else if (id.indexOf("d") == 0) {
                  // 有字母d代表是常数
                  val s = id.replaceAll("d", "")
                  value.append(s)
                  realValue.append(s)
                  rate.append(s)
                } else {
                  value.append(id)
                  realValue.append(id)
                  rate.append(id)
                }
              }
              val formulaUtil = new FormulaUtil
              var d_value = formulaUtil.getResult(value.toString())
              val d_realValue = formulaUtil.getResult(realValue.toString())
              val d_rate = formulaUtil.getResult(rate.toString())

              var maxValue = itemJson.getInteger(4)
              if (maxValue == null) maxValue = 10000
              var error = if (d_value <= maxValue) 0 else 2
              if (d_value < 0) {
                d_value = 0
                error = 2
              }
              // 记录计算好的虚拟表记录
              val itemName = s"${itemJson.getString(0)}_${itemJson.getString(1)}_${itemJson.getString(2)}"
              val hourMapJson = new JSONArray
              hourMapJson.add(itemName)
              hourMapJson.add(beforhour)
              hourMapJson.add(d_value)
              hourMapJson.add(d_realValue)
              hourMapJson.add(d_rate)
              hourMapJson.add(error)
              hourMapJson.add(itemJson.getString(7))
              // 查询小时表中虚拟表数据，计算小时表新旧记录的差值，累加到天表和月表
              val lastVirHourArray = PhoenixFunctions.getEnergyDataByTime(hourTableName, beforhour, null, itemName)
              // 更新到小时表
              try {
                PhoenixHelper.upsert(
                  connection,
                  hourTableName,
                  hourMapJson,
                  CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + hourTableName))
                log.info(s"Partition-${TaskContext.getPartitionId()}:更新虚拟表数据到小时表成功，数据为：${hourMapJson}")
              } catch {
                case t: Throwable =>
                  t.printStackTrace() // TODO: handle error
                  log.error(s"写入表${hourTableName}失败！出错数据为：$hourMapJson");
              }
              
              // 更新到天表和月表
              var changeValue = d_value
              var changeRealValue = d_realValue
              var changeRate = d_rate
              if (!lastVirHourArray.isEmpty) {
                val lastVirJson = lastVirHourArray.head
                changeValue = SparkFunctions.getSubtraction(lastVirJson.getDouble(2), changeValue)
                changeRealValue = SparkFunctions.getSubtraction(lastVirJson.getDouble(3), changeRealValue)
                changeRate = SparkFunctions.getSubtraction(lastVirJson.getDouble(4), changeRate)
              }
              // 获取天表上次记录
              val lastVirDayArray = PhoenixFunctions.getEnergyDataByTime(dayTableName, time.currentDayTime, null, itemName)
              var currentDayVirJson: JSONArray = null
              if (lastVirDayArray.isEmpty) {
                currentDayVirJson = new JSONArray
                currentDayVirJson.add(itemName)
                currentDayVirJson.add(time.currentDayTime)
                currentDayVirJson.add(changeValue)
                currentDayVirJson.add(changeRealValue)
                currentDayVirJson.add(changeRate)
                currentDayVirJson.add(error)
                currentDayVirJson.add(0d)
                currentDayVirJson.add(0d)
                currentDayVirJson.add(itemJson.getString(7))
              } else {
                currentDayVirJson = lastVirDayArray.head
                currentDayVirJson.set(2, SparkFunctions.getSum(currentDayVirJson.getDouble(2), changeValue))
                currentDayVirJson.set(3, SparkFunctions.getSum(currentDayVirJson.getDouble(3), changeRealValue))
                currentDayVirJson.set(4, SparkFunctions.getSum(currentDayVirJson.getDouble(4), changeRate))
              }
              // 更新到天表
              try {
                PhoenixHelper.upsert(
                  connection,
                  dayTableName,
                  currentDayVirJson,
                  CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + dayTableName))
                log.info(s"Partition-${TaskContext.getPartitionId()}:更新虚拟表数据到天表成功，数据为：${currentDayVirJson}")
              } catch {
                case t: Throwable =>
                  t.printStackTrace() // TODO: handle error
                  log.error(s"写入表${dayTableName}失败！出错数据为：$currentDayVirJson");
              }
              
              // 获取月表上次记录
              val lastVirMonthArray = PhoenixFunctions.getEnergyDataByTime(monthTableName, time.currentMonthTime, null, itemName)
              var currentMonthVirJson: JSONArray = null
              if (lastVirMonthArray.isEmpty) {
                currentMonthVirJson = new JSONArray
                currentMonthVirJson.add(itemName)
                currentMonthVirJson.add(time.currentMonthTime)
                currentMonthVirJson.add(changeValue)
                currentMonthVirJson.add(changeRealValue)
                currentMonthVirJson.add(changeRate)
                currentMonthVirJson.add(error)
                currentMonthVirJson.add(itemJson.getString(7))
              } else {
                currentMonthVirJson = lastVirMonthArray.head
                currentMonthVirJson.set(2, SparkFunctions.getSum(currentMonthVirJson.getDouble(2), changeValue))
                currentMonthVirJson.set(3, SparkFunctions.getSum(currentMonthVirJson.getDouble(3), changeRealValue))
                currentMonthVirJson.set(4, SparkFunctions.getSum(currentMonthVirJson.getDouble(4), changeRate))
              }
              // 更新到月表
              try {
                PhoenixHelper.upsert(
                  connection,
                  monthTableName,
                  currentMonthVirJson,
                  CleaningModule.getColumnsType(PhoenixFunctions.DATA_NAMESPACE + "~" + monthTableName))
                log.info(s"Partition-${TaskContext.getPartitionId()}:更新虚拟表数据到月表成功，数据为：${currentMonthVirJson}")
              } catch {
                case t: Throwable =>
                  t.printStackTrace() // TODO: handle error
                  log.error(s"写入表${monthTableName}失败！出错数据为：$currentMonthVirJson");
              }
            })
          }
          try {
        		if(connection != null) connection.close()
        	} catch {
        	case t: Throwable => t.printStackTrace() // TODO: handle error
        	}
        })
        log.info("计算虚拟表数据完成")
        // 删除data_access表中type为0的数据
        PhoenixFunctions.deleteDataAccess(allTime.lastHourTime, "0")
        log.info(s"删除data_access表中type为0,${allTime.lastHourTime}之前的数据")
      }

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
        log.info("检测到Flag为true，将要停止spark任务！")
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