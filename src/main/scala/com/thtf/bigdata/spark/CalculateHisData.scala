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

/**
 * 重新计算DS_HisData中某一段时间的数据，存入对应表中。
 */
object CalculateHisData {
  def main(args: Array[String]): Unit = {

    // Logger.getLogger("org").setLevel(Level.ERROR)
    val log = LoggerFactory.getLogger(this.getClass)

    // spark配置
    val master = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_MASTER)
    // 其他配置
    val errorFromTime = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_ERRORDATA_FROMTIME)
    val errorEndTime = PropertiesUtils.getPropertiesByKey(PropertiesConstant.SPARK_ERRORDATA_ENDTIME)

    var sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    // 测试用master
    if (master != null && master != "") {
      sparkConf = sparkConf.setMaster(master)
    }
    val sc = new SparkContext(sparkConf)

    // 如果配置的时间不正确，直接结束
    if (SparkFunctions.checkStringTime(errorFromTime, errorEndTime) == false) return

    /*
		 * 创建广播变量
		 */
    // (code,(basic_code,id))     (01-1,(A,11))
    val bcItemType = sc.broadcast(PhoenixFunctions.getItemTypeTable())
    println(bcItemType.value.size)
    // 表的最大值
    val bcMaxValue = sc.broadcast(PhoenixFunctions.getItemMaxValue())
    println(bcMaxValue.value.size)
    // 虚拟设备列表
    val bcVirtualItem = sc.broadcast(PhoenixFunctions.getVirtualItemList())
    println(bcVirtualItem.value.size)
    // ItemCode Map(id-itemCode)
    val bcItemCodeById = sc.broadcast(PhoenixFunctions.getItemCodeById())
    println(bcItemCodeById.value.size)
    // tbl_subentry Map(itemCode-subentry code) 010、01A、01B
    val bcSubentryCode = sc.broadcast(PhoenixFunctions.getSubentryMap())
    println(bcSubentryCode.value.size)
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
    var allTime = SparkFunctions.getAllTime(errorFromTime)
    var calculateFromTime = allTime.currentHourTime.replaceAll("\\D", "").toLong
    val calculateEndTime = errorEndTime.replaceAll("\\D", "").toLong

    println("程序处理时间范围为" + errorFromTime + "~" + errorEndTime)
    // 是否计算到了配置的errorEndTime
    while (calculateFromTime < calculateEndTime) {

      // 读取DS_HisData表对应时间的历史数据
      val hisDataResultArr = PhoenixFunctions.getHisDataByTime(calculateFromTime.toString(), allTime.nextHourTime.replaceAll("\\D", ""))
      println("本次查询历史表时间范围为" + calculateFromTime + "~" + allTime.nextHourTime.replaceAll("\\D", "") + "，查询记录数量为" + hisDataResultArr.length)
      hisDataResultArr.foreach(println)

      if (!hisDataResultArr.isEmpty) {
        // 处理查询的数据
        val hourDataResult = sc.parallelize(hisDataResultArr).filter(json => {
          json.getString(0) != null && // BuildingID
            json.getString(1) != null && // GateID
            json.getString(2) != null && // MeterID
            json.getString(3) != null && // ParamID
            json.getString(6) != null && // Status
            SparkFunctions.checkStringNumber(json.getString(7)) && // Value
            SparkFunctions.checkStringTime(json.getString(9)) // Timestamp
        }).groupBy(json => {
          (s"${json.getString(0)}_${json.getString(1)}_${json.getString(2)}", json.getString(3))
        }).mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val keyTypeValues = partIt.next()
            val itemName = keyTypeValues._1._1
            val code = keyTypeValues._1._2 // 01-1
            val typeCode = bcItemType.value.getOrElseUpdate(code, (null, null))._1 // A、B、C
            val typeId = bcItemType.value.getOrElseUpdate(code, (null, null))._2 // 11、12、13
            // 只计算电水燃气，A、B、C
            if (typeCode != null) {
              val valueJsonArray = keyTypeValues._2
                .toArray
                .filter(_.getString(6).trim() == "0") // 只计算status为0的数据
                .sortBy(_.getString(9).trim())
              if (!valueJsonArray.isEmpty) {
                val headJson = valueJsonArray.head
                val lastJson = valueJsonArray.last
                // 记录current_info数据
                val currentInfoJson = new JSONArray
                currentInfoJson.add(itemName)
                currentInfoJson.add(allTime.nextHourTime)
                currentInfoJson.add(lastJson.getString(7))
                currentInfoJson.add(lastJson.getString(6))
                currentInfoJson.add(typeId)
                // 添加current_info数据到累加器中
                currentInfoAccu.add(currentInfoJson)
                //
                // 记录小时数据
                val headValue = headJson.getString(7)
                val lastValue = lastJson.getString(7)
                var maxValue = bcMaxValue.value.getOrElse((itemName, typeId), 10000d)
                var energyValue = SparkFunctions.getSubtraction(headValue, lastValue)
                var state = if (energyValue < maxValue) "0" else "2"
                if (energyValue < 0) {
                  energyValue = 0
                  state = "2"
                }
                val totalRate = "0"
                val hourResultJson = new JSONArray
                hourResultJson.add(itemName)
                hourResultJson.add(allTime.currentHourTime)
                hourResultJson.add(energyValue)
                hourResultJson.add(valueJsonArray.last.getString(7))
                hourResultJson.add(totalRate)
                hourResultJson.add(state)
                hourResultJson.add(typeCode)
                // 添加到mapPartition中
                resultPart.append(hourResultJson)
                //
                // 记录到data_access数据，会有重复的数据，更新的到表的时候去重。
                val dataAccessJson = new JSONArray
                dataAccessJson.add(itemName.split("_")(0))
                dataAccessJson.add(itemName.split("_")(1))
                dataAccessJson.add(allTime.currentHourTime.replaceAll("\\D", "").take(10))
                dataAccessJson.add("0")
                // 添加data_access数据到累加器中
                dataAccessAccu.add(dataAccessJson)
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
        println("写入的电小时数据")
        hourDataResult.filter(json => { pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // other
                PhoenixFunctions.phoenixWriteHbase(
                  PhoenixFunctions.DATA_NAMESPACE,
                  PhoenixFunctions.other_hour_table,
                  hourDataResult.filter(json => {
                    !pattern.matcher(json.getString(6)).matches()
                  }))
        println("写入的other小时数据")
        hourDataResult.filter(json => { !pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // 更新tbl_item_current_info表
                PhoenixFunctions.updateCurrentInfo(currentInfoAccu.value)
        println("更新的tbl_item_current_info表")
        currentInfoAccu.value.foreach(println)
        // 清空累加器
        currentInfoAccu.reset()
        // 写入data_access表
                PhoenixFunctions.insertDataAccess(dataAccessAccu.value)
        println("更新的data_access表")
        dataAccessAccu.value.foreach(println)
        // 清空累加器
        dataAccessAccu.reset()

        // 计算分项小时表数据(在计算虚拟表之前)
        val elecHourData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.elec_hour_table, allTime.currentHourTime)
        println("查询到的小时数据")
        elecHourData.foreach(println)
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
              val rate = ""
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
                    val dataValue = json.getString(2)
                    if (SparkFunctions.checkStringNumber(dataValue)) {
                      if (subCode == "010") value = SparkFunctions.getSum(value.toString(), dataValue)
                      if (subCode == "01A") value_a = SparkFunctions.getSum(value_a.toString(), dataValue)
                      if (subCode == "01B") value_b = SparkFunctions.getSum(value_b.toString(), dataValue)
                      if (subCode == "01C") value_c = SparkFunctions.getSum(value_c.toString(), dataValue)
                      if (subCode == "01D") value_d = SparkFunctions.getSum(value_d.toString(), dataValue)
                    }
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
              buildingResult.add(allTime.currentHourTime)
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
        println("计算好的分项小时数据")
        subHourData.foreach(println)

        // 计算虚拟表数据
        // 两个小时之前
        // 获取前一个小时有更新的data_access
        // 确定已经处理了当前小时的数据，所以直接计算当前小时的虚拟表数据。在实时处理中逻辑有所差异。
        val dataAccessArray = PhoenixFunctions.getDataAccessList(calculateFromTime.toString())
        println("查询到的data_access表信息")
        dataAccessArray.foreach(println)
        val virtualHourData = sc.parallelize(dataAccessArray).filter(json => {
          SparkFunctions.checkStringTime(json.getString(3))
        }).mapPartitions(partIt => {
          val resultPart = new ArrayBuffer[JSONArray]
          while (partIt.hasNext) {
            val dataAccessJson = partIt.next()
            val buildingId = dataAccessJson.getString(1)
            // data_access中记录的时间
            val time = dataAccessJson.getString(3).trim()
            // 获取记录时间的前一个小时
            // 此处直接使用记录时间
            val beforhour = time + "0000"
            // 获取虚拟设备列表
            val virtualItemList = bcVirtualItem.value.filter(json => {
              json.getString(0) == buildingId
            })
            virtualItemList.foreach(itemJson => {
              val typeCode = itemJson.getString(5)
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
                  val itemCode = bcItemCodeById.value.getOrElse(id, null)
                  if (itemCode != null) {
                    // 获取对应小时数据
                    val valueMap = PhoenixFunctions.getHourData(hourTableName, itemCode, beforhour, "0")
                    if (!valueMap.isEmpty) {
                      val mapValue = valueMap.head.getString(0)
                      val mapRealValue = valueMap.head.getString(1)
                      val mapRate = valueMap.head.getString(2)
                      value.append(if (mapValue == null) "0" else mapValue.toString())
                      realValue.append(if (mapRealValue == null) "0" else mapRealValue.toString())
                      rate.append(if (mapRate == null) "0" else mapRate.toString())
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

              val maxValueStr = itemJson.getString(4)
              val maxValue = if (SparkFunctions.checkStringNumber(maxValueStr)) maxValueStr.trim().toDouble else 10000
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
        println("计算好的虚拟表数据-电")
        virtualHourData.filter(json => { pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // other
                PhoenixFunctions.phoenixWriteHbase(
                  PhoenixFunctions.DATA_NAMESPACE,
                  PhoenixFunctions.other_hour_table,
                  virtualHourData.filter(json => {
                    !pattern.matcher(json.getString(6)).matches()
                  }))
        println("计算好的虚拟表数据-other")
        virtualHourData.filter(json => { !pattern.matcher(json.getString(6)).matches() }).foreach(println)
        // 更新data_access表
                PhoenixFunctions.updateDataAccess(calculateFromTime.toString(), -1)
      }

      // 计算下一小时数据
      calculateFromTime = allTime.nextHourTime.replaceAll("\\D", "").toLong
      allTime = SparkFunctions.getAllTime(calculateFromTime.toString())
    }
    // 所有小时表数据计算完成

    // 计算天表数据
    allTime = SparkFunctions.getAllTime(errorFromTime)
    calculateFromTime = allTime.currentDayTime.replaceAll("\\D", "").toLong
    while (calculateFromTime < calculateEndTime) {

      println(s"计算天表本次查询范围${allTime.currentDayTime}~${allTime.nextDayTime}")
      // 查询电小时表一天的数据
      val elecOneDayData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.elec_hour_table, allTime.currentDayTime, allTime.nextDayTime)
      println("查询到的电表一天的小时数据")
      elecOneDayData.foreach(println)
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
            val hourData = partNext._2.iterator.toArray.sortBy(_.getString(1))
            var value = 0d
            var rate = 0d
            var error = 0
            var workValue = 0
            var otherValue = 0
            var typeCode: String = null
            val realValue = hourData.last.getString(3)
            for (json <- hourData) {
              val dataValue = json.getString(2)
              val dataRate = json.getString(4)
              typeCode = json.getString(6)
              if (SparkFunctions.checkStringNumber(dataValue)) {
                value = SparkFunctions.getSum(value.toString(), dataValue)
              }
              if (SparkFunctions.checkStringNumber(dataRate)) {
                rate = SparkFunctions.getSum(rate.toString(), dataRate)
              }
            }
            // 记录天数据
            val dayDataJson = new JSONArray
            dayDataJson.add(itemCode)
            dayDataJson.add(allTime.currentDayTime)
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
      println("计算好的电天表数据")
      elecDayResult.foreach(println)

      // 查询other小时表一天的数据
      val otherOneDayData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.other_hour_table, allTime.currentDayTime, allTime.nextDayTime)
      println("查询到的other表一天的小时数据")
      otherOneDayData.foreach(println)
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
            val hourData = partNext._2.iterator.toArray.sortBy(_.getString(1))
            var value = 0d
            var rate = 0d
            var error = 0
            var workValue = 0
            var otherValue = 0
            var typeCode: String = null
            val realValue = hourData.last.getString(3)
            for (json <- hourData) {
              val dataValue = json.getString(2)
              val dataRate = json.getString(4)
              typeCode = json.getString(6)
              if (SparkFunctions.checkStringNumber(dataValue)) {
                value = SparkFunctions.getSum(value.toString(), dataValue)
              }
              if (SparkFunctions.checkStringNumber(dataRate)) {
                rate = SparkFunctions.getSum(rate.toString(), dataRate)
              }
            }
            // 记录天数据
            val dayDataJson = new JSONArray
            dayDataJson.add(itemCode)
            dayDataJson.add(allTime.currentDayTime)
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
      println("计算好的other天表数据")
      otherDayResult.foreach(println)

      // 查询分项小时表一天的数据
      val subOneDayData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.subentry_hour_table, allTime.currentDayTime, allTime.nextDayTime)
      println("查询到的分项表一天的小时数据")
      subOneDayData.foreach(println)
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
            val hourData = partNext._2.iterator.toArray.sortBy(_.getString(1))
            // 记录天数据
            val dayDataJson = new JSONArray
            dayDataJson.set(0, itemCode)
            dayDataJson.set(1, 0d)
            dayDataJson.set(2, 0d)
            dayDataJson.set(3, 0d)
            dayDataJson.set(4, 0d)
            dayDataJson.set(5, 0d)
            dayDataJson.set(6, 0d)
            dayDataJson.set(7, allTime.currentDayTime)
            for (json <- hourData) {
              for (i <- 1 to 5) {
                val dataValue = json.getString(i)
                if (SparkFunctions.checkStringNumber(dataValue)) {
                  dayDataJson.set(i, SparkFunctions.getSum(dayDataJson.getString(i), dataValue))
                }
              }
              val dataRate = json.getString(6)
              if (SparkFunctions.checkStringNumber(dataRate)) {
                dayDataJson.set(6, SparkFunctions.getSum(dayDataJson.getString(6), dataRate))
              }
            }
            // 天数据放入mapPartition
            resultPart.append(dayDataJson)
          }
          resultPart.toIterator
        }).collect()
      // 分项-天数据存入天表
            PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.subentry_day_table, subDayResult)
      println("计算好的分项天表数据")
      subDayResult.foreach(println)

      // 计算下一天的数据
      calculateFromTime = allTime.nextDayTime.replaceAll("\\D", "").toLong
      allTime = SparkFunctions.getAllTime(calculateFromTime.toString())
    }
    // 所有天表数据计算完成

    // 计算月表数据
    allTime = SparkFunctions.getAllTime(errorFromTime)
    calculateFromTime = allTime.currentMonthTime.replaceAll("\\D", "").toLong
    while (calculateFromTime < calculateEndTime) {

      println(s"月数据本次查询范围${allTime.currentMonthTime}~${allTime.nextMonthTime}")
      // 查询电天表一个月的数据
      val elecOneMonthData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.elec_day_table, allTime.currentMonthTime, allTime.nextMonthTime)
      println("查询到的电一个月的数据")
      elecOneMonthData.foreach(println)
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
            val dayData = partNext._2.iterator.toArray.sortBy(_.getString(1))
            var value = 0d
            var rate = 0d
            var error = 0
            var typeCode: String = null
            val realValue = dayData.last.getString(3)
            for (json <- dayData) {
              val dataValue = json.getString(2)
              val dataRate = json.getString(4)
              typeCode = json.getString(6)
              if (SparkFunctions.checkStringNumber(dataValue)) {
                value = SparkFunctions.getSum(value.toString(), dataValue)
              }
              if (SparkFunctions.checkStringNumber(dataRate)) {
                rate = SparkFunctions.getSum(rate.toString(), dataRate)
              }
            }
            // 记录月数据
            val dayDataJson = new JSONArray
            dayDataJson.add(itemCode)
            dayDataJson.add(allTime.currentDayTime)
            dayDataJson.add(value)
            dayDataJson.add(realValue)
            dayDataJson.add(rate)
            dayDataJson.add(error)
            dayDataJson.add(typeCode)
            // 月数据放入mapPartition
            resultPart.append(dayDataJson)
          }
          resultPart.toIterator
        }).collect()
      // 电-月数据存入月表
            PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.elec_month_table, elecMonthResult)
      println("计算好的电月表数据")
      elecMonthResult.foreach(println)

      // 查询other天表一个月的数据
      val otherOneMonthData = PhoenixFunctions.getEnergyDataByTime(PhoenixFunctions.other_day_table, allTime.currentMonthTime, allTime.nextMonthTime)
      println("查询到的一个月的other数据")
      otherOneMonthData.foreach(println)
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
            val dayData = partNext._2.iterator.toArray.sortBy(_.getString(1))
            var value = 0d
            var rate = 0d
            var error = 0
            var typeCode: String = null
            val realValue = dayData.last.getString(3)
            for (json <- dayData) {
              val dataValue = json.getString(2)
              val dataRate = json.getString(4)
              typeCode = json.getString(6)
              if (SparkFunctions.checkStringNumber(dataValue)) {
                value = SparkFunctions.getSum(value.toString(), dataValue)
              }
              if (SparkFunctions.checkStringNumber(dataRate)) {
                rate = SparkFunctions.getSum(rate.toString(), dataRate)
              }
            }
            // 记录月数据
            val dayDataJson = new JSONArray
            dayDataJson.add(itemCode)
            dayDataJson.add(allTime.currentDayTime)
            dayDataJson.add(value)
            dayDataJson.add(realValue)
            dayDataJson.add(rate)
            dayDataJson.add(error)
            dayDataJson.add(typeCode)
            // 月数据放入mapPartition
            resultPart.append(dayDataJson)
          }
          resultPart.toIterator
        }).collect()
      // other-月数据存入月表
            PhoenixFunctions.phoenixWriteHbase(PhoenixFunctions.DATA_NAMESPACE, PhoenixFunctions.other_month_table, otherMonthResult)
      println("计算好的other月表数据")
      otherMonthResult.foreach(println)

      // 计算下一个月的数据
      calculateFromTime = allTime.nextMonthTime.replaceAll("\\D", "").toLong
      allTime = SparkFunctions.getAllTime(calculateFromTime.toString())
    }

  }
}