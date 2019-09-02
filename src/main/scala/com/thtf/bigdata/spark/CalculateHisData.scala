package com.thtf.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import com.thtf.bigdata.util.PropertiesUtils
import com.thtf.bigdata.common.PropertiesConstant
import com.thtf.bigdata.functions.SparkFunctions
import com.thtf.bigdata.functions.PhoenixFunctions
import com.alibaba.fastjson.JSONArray
import com.thtf.bigdata.spark.accumulator.JsonAccumulator
import com.thtf.bigdata.spark.accumulator.JsonAccumulator
import java.util.regex.Pattern
import com.thtf.bigdata.util.FormulaUtil
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import java.util.Date
import com.thtf.bigdata.kafka.ZkKafkaOffsetManager
import com.thtf.bigdata.entity.DateTimeEntity
import com.thtf.bigdata.common.TableConstant

/**
 * 重新计算DS_HisData中某一段时间的数据，存入对应表中。
 */
object CalculateHisData {
  
  // yyyyMMddHHmmss -> yyyy-MM-dd HH:mm:ss
  def getyy_MM_ddTime(time:String) = {
    val yy_MM_dd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		val yyMMdd = new SimpleDateFormat("yyyyMMddHHmmss")
    yy_MM_dd.format(yyMMdd.parse(time.replaceAll("\\D", "").dropRight(4) + "0000"))
  }
  def getyy_MM_ddTime(time:Long) = {
		  val yy_MM_dd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		  yy_MM_dd.format(new Date(time))
  }
  def getTimestamp(time:String) = {
    new SimpleDateFormat("yyyyMMddHHmmss").parse(time.replaceAll("\\D", "").dropRight(4) + "0000").getTime
  }
  
  def main(args: Array[String]): Unit = {

    // Logger.getLogger("org").setLevel(Level.ERROR)
    val log = LoggerFactory.getLogger(this.getClass)

    // spark配置
    val master = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_MASTER)
    val groupId = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_CALCULATEHISDATA_BEGINTIME)
    val topic = "errorFromTime"
    // zk配置
    val zkUrl = PropertiesUtils.getPropertiesByKey(PropertiesConstant.ZOOKEEPER_URL)
    // 其他配置
    val errorFromTime = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_ERRORDATA_FROMTIME)
    val errorEndTime = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_ERRORDATA_ENDTIME)
    
    val currentTableName = PropertiesUtils.getPropertiesByKey(TableConstant.CURRENT_INFO_HIS)

    var sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    // 测试用master
    if (master != null && master != "") {
      sparkConf = sparkConf.setMaster(master)
    }
    val sc = new SparkContext(sparkConf)

    // 如果配置的时间不正确，直接结束
    if (SparkFunctions.checkStringTime(errorFromTime, errorEndTime) == false) {
      log.error("配置的时间不正确，请重新配置")
      return
    }

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
    //    println(bcItemType.value.size)
    //    println(bcMaxValue.value.size)
    //    println(bcVirtualItem.value.size)
    //    println(bcItemCodeById.value.size)
    //    println(bcSubentryCode.value.size)
    /*
		 * 创建累加器
		 */
    // tbl_item_current_info
    val currentInfoAccu = new JsonAccumulator
    sc.register(currentInfoAccu)
    // data_access
    val dataAccessAccu = new JsonAccumulator
    sc.register(dataAccessAccu)

    /*
		 * 计算小时数据
		 * 1、从配置的开始时间起，每次读取一小时历史数据，计算小时数据
		 * 2、更新tbl_item_current_info表，记录最新数据
		 * 3、更新data_access表，记录有更新的采集器
		 * 4、根据data_access表，计算虚拟表数据到小时表
		 */
    // 20190720050000   2019 0720 05 00 00
    
    val zkTimeRecordManager = new ZkKafkaOffsetManager(zkUrl)
    val lastTimeRecord = zkTimeRecordManager.readBeginTime(Seq(topic), groupId)
    var allTime: DateTimeEntity = null
    if(lastTimeRecord == null){
    	allTime = SparkFunctions.getAllTime(errorFromTime)
    }else {
    	allTime = SparkFunctions.getAllTime(lastTimeRecord)
    }
    var calculateFromTime = allTime.currentHourTime.replaceAll("\\D", "").toLong
    val calculateEndTime = errorEndTime.replaceAll("\\D", "").toLong

    if(lastTimeRecord == null){
    	log.info("程序处理时间范围为" + errorFromTime + "~" + errorEndTime)
    }else {
    	log.info("从上次程序结束记录的时间开始处理，处理时间范围为" + lastTimeRecord + "~" + errorEndTime)
    }
    // 是否计算到了配置的errorEndTime
    while (calculateFromTime <= calculateEndTime) {

      // 读取DS_HisData表对应时间的历史数据
      val hisDataResultArr = PhoenixFunctions.getHisDataByTime(calculateFromTime.toString(), allTime.nextHourTime.replaceAll("\\D", ""))
      log.info("本次查询历史表时间范围为" + allTime.currentHourTime + "~" + allTime.nextHourTime + "，查询记录数量为" + hisDataResultArr.length)
      //      hisDataResultArr.foreach(println)
      
      // 创建广播变量
      // 获取tbl_item_current_info表记录
      val bcCurrentInfo = sc.broadcast(PhoenixFunctions.getCurrentInfoMap(currentTableName))

      if (!hisDataResultArr.isEmpty) {
        // 处理查询的数据
        val hourDataResult = sc.parallelize(hisDataResultArr).filter(json => {
          json.getString(0) != null && // BuildingID
            json.getString(1) != null && // GateID
            json.getString(2) != null && // MeterID
            json.getString(3) != null && // ParamID
            json.getString(6) != null && // Status
            SparkFunctions.checkStringNumber(json.getString(7)) && // Value 历史表中的varchar类型的数值需要检验
            SparkFunctions.checkStringTime(json.getString(9)) // Timestamp
        }).groupBy(json => {
          (s"${json.getString(0)}_${json.getString(1)}_${json.getString(2)}", json.getString(3))
        }).mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val keyTypeValues = partIt.next()
            val itemName = keyTypeValues._1._1
            val code = keyTypeValues._1._2 // 01-1
            // 只计算tbl_item_type表中有的
            val hasItemType = bcItemType.value.getOrElseUpdate(code, null)
            if (hasItemType != null) {
              val typeCode = hasItemType._1 // A、B、C、null
              val typeId = hasItemType._2.toString() // 11、12、13、14、15
              val valueJsonArray = keyTypeValues._2
                .toArray
                .filter(_.getString(6).trim() == "0") // 只计算status为0的数据
                .sortBy(json => getTimestamp(json.getString(9)))
              if (!valueJsonArray.isEmpty) {
                // 获取item_name上一次数据记录
                var lastCurrentInfo = bcCurrentInfo.value.getOrElse((itemName,typeId),null)
                // 获取item信息
                var maxValueAndcoefficient = bcMaxValue.value.getOrElse((itemName, typeId), null)
          			var maxValue: Double = 10000
          			var coefficient: Double = 1
          			if(maxValueAndcoefficient != null){
          			  maxValue = maxValueAndcoefficient._1
          			  coefficient = maxValueAndcoefficient._2
          			}
            		val headJson = valueJsonArray.head
            		val headJsonTime = headJson.getString(9)
            		// 记录current_info数据
                val currentInfoJson = new JSONArray
                // 如果没有上一次的记录，或者本次第一条数据时间超过上次一小时，则记录本次查询第一条数据，不计算
                if(lastCurrentInfo == null || getTimestamp(headJsonTime) - getTimestamp(lastCurrentInfo._1) > 3600000){
            			val headValue = SparkFunctions.getProduct(headJson.getString(7),coefficient.toString())
                  lastCurrentInfo = (getyy_MM_ddTime(headJsonTime),headValue)
                  currentInfoJson.add(itemName)
                  currentInfoJson.add(getyy_MM_ddTime(headJsonTime))
                  currentInfoJson.add(headValue)
                  currentInfoJson.add(headJson.getString(6))
                  currentInfoJson.add(typeId)
                  // 添加current_info数据到累加器中
                  currentInfoAccu.add(currentInfoJson)
                }else {
                  var flag = true
                  for(i <- 0 until valueJsonArray.length if flag){
                	  val nextJson = valueJsonArray(i)
                	  // 如果相差一小时，计算小时数据，并记录最新currentInfo
                	  if(getTimestamp(nextJson.getString(9)) - getTimestamp(lastCurrentInfo._1) == 3600000){
                    	// 记算小时数据
                    	val currentInfoValue = lastCurrentInfo._2
                			val nextValue = SparkFunctions.getProduct(nextJson.getString(7),coefficient.toString())
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
                			resultPart.append(hourResultJson)
                			//
                			currentInfoJson.add(itemName)
                      currentInfoJson.add(getyy_MM_ddTime(nextJson.getString(9)))
                      currentInfoJson.add(nextValue)
                      currentInfoJson.add(state)
                      currentInfoJson.add(typeId)
                      // 添加current_info数据到累加器中
                      currentInfoAccu.add(currentInfoJson)
                			// 记录到data_access数据，会有重复的数据，更新的到表的时候去重。
                			val dataAccessJson = new JSONArray
                			dataAccessJson.add(itemName.split("_")(0))
                			dataAccessJson.add(itemName.split("_")(1))
                			dataAccessJson.add(allTime.currentHourTime.replaceAll("\\D", "").take(10))
                			dataAccessJson.add("0")
                			// 添加data_access数据到累加器中
                			dataAccessAccu.add(dataAccessJson)
                			flag = false
                	  }
                  }
                }
              }
            }
          }
          resultPart.toIterator
        }).collect()

        // 写入计算好的小时数据
        val pattern = Pattern.compile(" *A *")
        // 电
        PhoenixFunctions.phoenixWriteHbase(
          PhoenixFunctions.DATA_NAMESPACE,
          PhoenixFunctions.elec_hour_table,
          hourDataResult.filter(json => {
            pattern.matcher(json.getString(6)).matches()
          }))
        log.info("写入电小时数据")
        //        hourDataResult.filter(json => { pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // other
        PhoenixFunctions.phoenixWriteHbase(
          PhoenixFunctions.DATA_NAMESPACE,
          PhoenixFunctions.other_hour_table,
          hourDataResult.filter(json => {
            !pattern.matcher(json.getString(6)).matches()
          }))
        log.info("写入other小时数据")
        //        hourDataResult.filter(json => { !pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // 更新tbl_item_current_info表
        PhoenixFunctions.updateCurrentInfo(currentInfoAccu.value,currentTableName)
        log.info("更新tbl_item_current_info表")
        //        currentInfoAccu.value.foreach(println)
        // 清空累加器
        currentInfoAccu.reset()
        // 写入data_access表
        PhoenixFunctions.insertDataAccess(dataAccessAccu.value)
        log.info("写入data_access表")
        //        dataAccessAccu.value.foreach(println)
        // 清空累加器
        dataAccessAccu.reset()

        // 计算分项小时表数据(在计算虚拟表之前)
        val elecHourData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.elec_hour_table, allTime.lastHourTime,null,null)
        log.info(s"计算分项小时表本次查询时间为：${allTime.lastHourTime},查询数据量为${elecHourData.length}")
        //        println("查询到的小时数据")
        //        elecHourData.foreach(println)
        val subHourData = sc.parallelize(elecHourData)
          .groupBy(_.getString(0).split("_")(0))
          .mapPartitions(partIt => {
            val resultPart = new ArrayBuffer[JSONArray]
            while (partIt.hasNext) {
              val buildingJsons = partIt.next()
              val buildingCode = buildingJsons._1
              var value = 0d
              var value_a = 0d
              var value_b = 0d
              var value_c = 0d
              var value_d = 0d
              // var dataTime = ""
              val rate = "0"
              val jsonArray = buildingJsons._2.iterator
              while (jsonArray.hasNext) {
                val json = jsonArray.next()
                // dataTime = json.getString(1)
                val error = json.getString(5)
                if (error != null && error.trim() == "0") {
                  val itemCode = json.getString(0)
                  var subCode = bcSubentryCode.value.getOrElse(itemCode, null)
                  if (subCode != null) {
                    subCode = subCode.trim().take(3)
                    val dataValue = json.getDouble(2)
                    if (subCode == "010") value = SparkFunctions.getSum(value, dataValue)
                    if (subCode == "01A") value_a = SparkFunctions.getSum(value_a, dataValue)
                    if (subCode == "01B") value_b = SparkFunctions.getSum(value_b, dataValue)
                    if (subCode == "01C") value_c = SparkFunctions.getSum(value_c, dataValue)
                    if (subCode == "01D") value_d = SparkFunctions.getSum(value_d, dataValue)
                  }
                }
              }
              // 记录计算分项数据
              val buildingResult = new JSONArray
              buildingResult.add(buildingCode)
              buildingResult.add(value)
              buildingResult.add(value_a)
              buildingResult.add(value_b)
              buildingResult.add(value_c)
              buildingResult.add(value_d)
              buildingResult.add(rate)
              buildingResult.add(allTime.lastHourTime)
              // 放入mapPartition中
              resultPart.append(buildingResult)
            }
            resultPart.toIterator
          }).collect()
        // 将分项小时数据写入分项小时表
        PhoenixFunctions.phoenixWriteHbase(
          PhoenixFunctions.DATA_NAMESPACE,
          PhoenixFunctions.subentry_hour_table,
          subHourData)
        log.info("写入分项小时数据")
        //        subHourData.foreach(println)
        // 删除data_access表中type为2的数据
        PhoenixFunctions.deleteDataAccess(allTime.lastHourTime, "2", "=")
        log.info("删除data_access表中type为2的数据")

        // 计算虚拟表数据
        // 两个小时之前
        // 获取前一个小时有更新的data_access
        // 确定已经得到了上一小时的数据，所以查询当前小时的虚拟表数据。在实时处理中逻辑有所差异。
        val dataAccessArray = PhoenixFunctions.getDataAccessList(calculateFromTime.toString())
        log.info(s"计算计算虚拟表本次查询data_access时间为：${allTime.currentHourTime},查询数据量为${dataAccessArray.length}")
        //        println("查询到的data_access表信息")
        //        dataAccessArray.foreach(println)
        val virtualHourData = sc.parallelize(dataAccessArray).filter(json => {
          SparkFunctions.checkStringTime(json.getString(2))
        }).mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val dataAccessJson = partIt.next()
            val buildingId = dataAccessJson.getString(0)
            // data_access中记录的时间
            val time = dataAccessJson.getString(2).trim()
            // 获取记录时间的前一个小时
            val beforhour = getyy_MM_ddTime(getTimestamp(time + "0000") - 3600000)
            // 获取虚拟设备列表
            val virtualItemList = bcVirtualItem.value.filter(json => {
              json.getString(0) == buildingId
            })
            virtualItemList.foreach(itemJson => {
              val typeCode = itemJson.getString(5) // 01-1、02-1、03-1
              val hourTableName = if (typeCode == "01-1") PhoenixFunctions.elec_hour_table else PhoenixFunctions.other_hour_table
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
              val hourMapJson = new JSONArray
              hourMapJson.add(s"${itemJson.getString(0)}_${itemJson.getString(1)}_${itemJson.getString(2)}")
              hourMapJson.add(beforhour)
              hourMapJson.add(d_value)
              hourMapJson.add(d_realValue)
              hourMapJson.add(d_rate)
              hourMapJson.add(error)
              hourMapJson.add(itemJson.getString(7))
              // 添加到mapPartition中
              resultPart.append(hourMapJson)
            })
          }
          resultPart.toIterator
        }).collect()
        // 写入计算好的虚拟表数据
        // 电
        PhoenixFunctions.phoenixWriteHbase(
          PhoenixFunctions.DATA_NAMESPACE,
          PhoenixFunctions.elec_hour_table,
          virtualHourData.filter(json => {
            pattern.matcher(json.getString(6)).matches()
          }))
        log.info("写入虚拟表数据-电")
        //        virtualHourData.filter(json => { pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // other
        PhoenixFunctions.phoenixWriteHbase(
          PhoenixFunctions.DATA_NAMESPACE,
          PhoenixFunctions.other_hour_table,
          virtualHourData.filter(json => {
            !pattern.matcher(json.getString(6)).matches()
          }))
        log.info("写入虚拟表数据-other")
        //        virtualHourData.filter(json => { !pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // 删除data_access表type为0的数据
        PhoenixFunctions.deleteDataAccess(allTime.currentHourTime, "0")
        log.info("删除data_access表中type为0的数据")
      }
      
      // 计算天数据
      if(allTime.currentHourTime == allTime.currentDayTime || allTime.currentHourTime == getyy_MM_ddTime(errorEndTime)){
        val flagTime = allTime.currentHourTime
        var dayFlag = false
        if(allTime.currentHourTime != allTime.currentDayTime && allTime.currentHourTime == getyy_MM_ddTime(errorEndTime)){
          allTime = SparkFunctions.getAllTime(allTime.nextDayTime)
          dayFlag = true
        }
      // 查询电小时表一天的数据
      val elecOneDayData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.elec_hour_table, allTime.lastDayTime, allTime.currentDayTime,null)
		  log.info(s"计算电天表本次查询范围${allTime.lastDayTime}~${allTime.currentDayTime},查询数据量为${elecOneDayData.length}")
      //      println("查询到的电表一天的小时数据")
      //      elecOneDayData.foreach(println)
      val elecDayResult = sc.parallelize(elecOneDayData)
        .filter(json => {
          json.getString(5) == "0" && SparkFunctions.checkStringTime(json.getString(1))
        })
        .groupBy(_.getString(0))
        .mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val partNext = partIt.next()
            val itemCode = partNext._1
            val hourData = partNext._2.iterator.toArray
            var value = 0d
            var rate = 0d
            var error = 0
            var workValue = 0
            var otherValue = 0
            var typeCode: String = null
            //            val realValue = hourData.last.getString(3)
            val realValue = hourData.last.getDouble(3)
            for (json <- hourData) {
              val dataValue = json.getDouble(2)
              val dataRate = json.getDouble(4)
              value = SparkFunctions.getSum(value, dataValue)
              rate = SparkFunctions.getSum(rate, dataRate)
              typeCode = json.getString(6)
            }
            // 记录天数据
            val dayDataJson = new JSONArray
            dayDataJson.add(itemCode)
            dayDataJson.add(allTime.lastDayTime)
            dayDataJson.add(value)
            dayDataJson.add(realValue)
            dayDataJson.add(rate)
            dayDataJson.add(error)
            dayDataJson.add(workValue)
            dayDataJson.add(otherValue)
            dayDataJson.add(typeCode)
            // 天数据放入mapPartition
            resultPart.append(dayDataJson)
          }
          resultPart.toIterator
        }).collect()
      // 电-天数据存入天表
      PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.elec_day_table, elecDayResult)
      log.info("写入电天表数据")
      //      elecDayResult.foreach(println)

      // 查询other小时表一天的数据
      val otherOneDayData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.other_hour_table, allTime.lastDayTime, allTime.currentDayTime,null)
      log.info(s"计算other天表本次查询范围${allTime.lastDayTime}~${allTime.currentDayTime},查询数据量为${otherOneDayData.length}")
      //      println("查询到的other表一天的小时数据")
      //      otherOneDayData.foreach(println)
      val otherDayResult = sc.parallelize(otherOneDayData)
        .filter(json => {
          json.getString(5) == "0" && SparkFunctions.checkStringTime(json.getString(1))
        })
        .groupBy(_.getString(0))
        .mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val partNext = partIt.next()
            val itemCode = partNext._1
            val hourData = partNext._2.iterator.toArray
            var value = 0d
            var rate = 0d
            var error = 0
            var workValue = 0
            var otherValue = 0
            var typeCode: String = null
            //            val realValue = hourData.last.getString(3)
            val realValue = hourData.last.getDouble(3)
            for (json <- hourData) {
              val dataValue = json.getDouble(2)
              val dataRate = json.getDouble(4)
              value = SparkFunctions.getSum(value, dataValue)
              rate = SparkFunctions.getSum(rate, dataRate)
              typeCode = json.getString(6)
            }
            // 记录天数据
            val dayDataJson = new JSONArray
            dayDataJson.add(itemCode)
            dayDataJson.add(allTime.lastDayTime)
            dayDataJson.add(value)
            dayDataJson.add(realValue)
            dayDataJson.add(rate)
            dayDataJson.add(error)
            dayDataJson.add(workValue)
            dayDataJson.add(otherValue)
            dayDataJson.add(typeCode)
            // 天数据放入mapPartition
            resultPart.append(dayDataJson)
          }
          resultPart.toIterator
        }).collect()
      // other-天数据存入天表
      PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.other_day_table, otherDayResult)
      log.info("写入other天表数据")
      //      otherDayResult.foreach(println)

      // 查询分项小时表一天的数据
      val subOneDayData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.subentry_hour_table, allTime.lastDayTime, allTime.currentDayTime,null)
      log.info(s"计算分项表天表本次查询范围${allTime.lastDayTime}~${allTime.currentDayTime},查询数据量为${subOneDayData.length}")
      //      println("查询到的分项表一天的小时数据")
      //      subOneDayData.foreach(println)
      val subDayResult = sc.parallelize(subOneDayData)
        .filter(json => {
          SparkFunctions.checkStringTime(json.getString(7))
        })
        .groupBy(_.getString(0))
        .mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val partNext = partIt.next()
            val itemCode = partNext._1
            val hourData = partNext._2.iterator.toArray
            // 记录天数据
            val dayDataJson = new JSONArray
            dayDataJson.set(0, itemCode)
            dayDataJson.set(1, 0d)
            dayDataJson.set(2, 0d)
            dayDataJson.set(3, 0d)
            dayDataJson.set(4, 0d)
            dayDataJson.set(5, 0d)
            dayDataJson.set(6, 0d)
            dayDataJson.set(7, allTime.lastDayTime)
            for (json <- hourData) {
              for (i <- 1 to 5) {
                val dataValue = json.getDouble(i)
                dayDataJson.set(i, SparkFunctions.getSum(dayDataJson.getDouble(i), dataValue))
              }
              val dataRate = json.getDouble(6)
              dayDataJson.set(6, SparkFunctions.getSum(dayDataJson.getDouble(6), dataRate))
            }
            // 天数据放入mapPartition
            resultPart.append(dayDataJson)
          }
          resultPart.toIterator
        }).collect()
      // 分项-天数据存入天表
      PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.subentry_day_table, subDayResult)
      log.info("写入分项天表数据")
      //      subDayResult.foreach(println)
      if(dayFlag){
        allTime = SparkFunctions.getAllTime(flagTime)
      }
      }
      // 所有天表数据计算完成
      
      // 计算月数据
      if(allTime.currentHourTime == allTime.currentMonthTime || allTime.currentHourTime == getyy_MM_ddTime(errorEndTime)){
        val flagTime = allTime.currentHourTime
        var monthFlag = false
        if(allTime.currentHourTime != allTime.currentMonthTime && allTime.currentHourTime == getyy_MM_ddTime(errorEndTime)){
          allTime = SparkFunctions.getAllTime(allTime.nextMonthTime)
          monthFlag = true
        }
      // 查询电天表一个月的数据
      val elecOneMonthData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.elec_day_table, allTime.lastMonthTime, allTime.currentMonthTime,null)
		  log.info(s"计算电月表本次查询范围${allTime.lastMonthTime}~${allTime.currentMonthTime},查询数据量为${elecOneMonthData.length}")
      //      println("查询到的电一个月的数据")
      //      elecOneMonthData.foreach(println)
      val elecMonthResult = sc.parallelize(elecOneMonthData)
        .filter(json => {
          json.getString(5) == "0" && SparkFunctions.checkStringTime(json.getString(1))
        })
        .groupBy(_.getString(0))
        .mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val partNext = partIt.next()
            val itemCode = partNext._1
            val dayData = partNext._2.iterator.toArray
            var value = 0d
            var rate = 0d
            var error = 0
            var typeCode: String = null
            //            val realValue = dayData.last.getString(3)
            val realValue = dayData.last.getDouble(3)
            for (json <- dayData) {
              val dataValue = json.getDouble(2)
              val dataRate = json.getDouble(4)
              value = SparkFunctions.getSum(value, dataValue)
              rate = SparkFunctions.getSum(rate, dataRate)
              typeCode = json.getString(6)
            }
            // 记录月数据
            val monthDataJson = new JSONArray
            monthDataJson.add(itemCode)
            monthDataJson.add(allTime.lastMonthTime)
            monthDataJson.add(value)
            monthDataJson.add(realValue)
            monthDataJson.add(rate)
            monthDataJson.add(error)
            monthDataJson.add(typeCode)
            // 月数据放入mapPartition
            resultPart.append(monthDataJson)
          }
          resultPart.toIterator
        }).collect()
      // 电-月数据存入月表
      PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.elec_month_table, elecMonthResult)
      log.info("写入电月表数据")
      //      elecMonthResult.foreach(println)

      // 查询other天表一个月的数据
      val otherOneMonthData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.other_day_table, allTime.lastMonthTime, allTime.currentMonthTime,null)
      log.info(s"计算other月表本次查询范围${allTime.lastMonthTime}~${allTime.currentMonthTime},查询数据量为${otherOneMonthData.length}")
      //      println("查询到的一个月的other数据")
      //      otherOneMonthData.foreach(println)
      val otherMonthResult = sc.parallelize(otherOneMonthData)
        .filter(json => {
          json.getString(5) == "0" && SparkFunctions.checkStringTime(json.getString(1))
        })
        .groupBy(_.getString(0))
        .mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val partNext = partIt.next()
            val itemCode = partNext._1
            val dayData = partNext._2.iterator.toArray
            var value = 0d
            var rate = 0d
            var error = 0
            var typeCode: String = null
            //            val realValue = dayData.last.getString(3)
            val realValue = dayData.last.getDouble(3)
            for (json <- dayData) {
              val dataValue = json.getDouble(2)
              val dataRate = json.getDouble(4)
              value = SparkFunctions.getSum(value, dataValue)
              rate = SparkFunctions.getSum(rate, dataRate)
              typeCode = json.getString(6)
            }
            // 记录月数据
            val monthDataJson = new JSONArray
            monthDataJson.add(itemCode)
            monthDataJson.add(allTime.lastMonthTime)
            monthDataJson.add(value)
            monthDataJson.add(realValue)
            monthDataJson.add(rate)
            monthDataJson.add(error)
            monthDataJson.add(typeCode)
            // 月数据放入mapPartition
            resultPart.append(monthDataJson)
          }
          resultPart.toIterator
        }).collect()
      // other-月数据存入月表
      PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.other_month_table, otherMonthResult)
      log.info("写入other月表数据")
      //      otherMonthResult.foreach(println)
      if(monthFlag){
        allTime = SparkFunctions.getAllTime(flagTime)
      }
      }
      // 本小时数据处理完成，记录下个小时时间，程序失败从下个小时开始计算
      zkTimeRecordManager.saveBeginTime(Seq(topic), groupId, allTime.nextHourTime)
      
      // 计算下一小时数据
      calculateFromTime = allTime.nextHourTime.replaceAll("\\D", "").toLong
      allTime = SparkFunctions.getAllTime(calculateFromTime.toString())
    }
    // 所有小时表数据计算完成
    
    

  }
}