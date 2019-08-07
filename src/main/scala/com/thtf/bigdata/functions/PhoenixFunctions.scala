package com.thtf.bigdata.functions

import com.thtf.bigdata.util.PropertiesUtils
import com.thtf.bigdata.common.PropertiesConstant
import com.thtf.bigdata.common.TableConstant
import com.thtf.bigdata.hbase.util.PhoenixHelper
import scala.collection.mutable.HashMap
import java.sql.ResultSet
import com.alibaba.fastjson.JSONArray
import java.util.ArrayList
import org.apache.log4j.Logger
import scala.collection.mutable.ArrayBuffer

object PhoenixFunctions {
  
  val log = Logger.getLogger(this.getClass)
  
  // 命名空间
  val INFO_NAMESPACE = PropertiesUtils.getPropertiesByKey(PropertiesConstant.HBASE_NAMESPACE)
  val DATA_NAMESPACE = PropertiesUtils.getPropertiesByKey(TableConstant.NAMESPACE)
  
  // 表名
  // 历史数据表名
  val hisdata_table = PropertiesUtils.getPropertiesByKey(TableConstant.HISDATA_TABLE)
  // 错误历史数据表名
  val hisdata_table_error = PropertiesUtils.getPropertiesByKey(TableConstant.HISDATA_TABLE_ERROR)
  // 电      小时表
  val elec_hour_table = PropertiesUtils.getPropertiesByKey(TableConstant.ELECTRICITY_HOUR)
  // other小时表
  val other_hour_table = PropertiesUtils.getPropertiesByKey(TableConstant.OTHER_HOUR)
  // 电         天表
  val elec_day_table = PropertiesUtils.getPropertiesByKey(TableConstant.ELECTRICITY_DAY)
  // other 天表
  val other_day_table = PropertiesUtils.getPropertiesByKey(TableConstant.OTHER_DAY)
  // 电        月表
  val elec_month_table = PropertiesUtils.getPropertiesByKey(TableConstant.ELECTRICITY_MONTH)
  // other 月表
  val other_month_table = PropertiesUtils.getPropertiesByKey(TableConstant.OTHER_MONTH)
  // 分项     小时表
  val subentry_hour_table = PropertiesUtils.getPropertiesByKey(TableConstant.SUBENTRY_HOUR)
  // 分项     天表
  val subentry_day_table = PropertiesUtils.getPropertiesByKey(TableConstant.SUBENTRY_DAY)
  // currnet_info表
  val current_info_table = PropertiesUtils.getPropertiesByKey(TableConstant.CURRENT_INFO)
  // data_access表
  val data_access_table = PropertiesUtils.getPropertiesByKey(TableConstant.DATA_ACCESS)
  // tbl_item_type表
//  val item_type_table = PropertiesUtils.getPropertiesByKey(TableConstant.ITEM_TYPE)
  
  
  /**
   * 通过phoenixHelper读取hbase表中的前一个小时的数据
   *
   * @param tablename:表名
   * @param cols:
   * @param time:当前整点时间和上个整点时间
   * @return
   */
  def getHisDataByTime(startTime: String, endTime: String) = {
    val columns = Array("BuildingID",
                        "GateID",
                        "MeterID",
                        "ParamID",
                        "Type",
                        "Name",
                        "Status",
                        "Value",
                        "Unit",
                        "Timestamp")
    val timeCol = "\"Timestamp\""
    var wheres = Array(timeCol + " >= '" + startTime + "'", timeCol + " < '" + endTime + "'")
    SparkFunctions.result2JsonArr(PhoenixHelper.query(DATA_NAMESPACE, hisdata_table, columns, wheres))
  }
  
  /**
   * 获取tbl_item_type表
   *
   * @return
   */
  def getItemTypeTable() = {
    // 查询的列
    val itemTableCols = Array[String]("code", "basic_code", "id")
    val itemResultSet = PhoenixHelper.query(INFO_NAMESPACE, "tbl_item_type", itemTableCols, null)
    // 读取item表，放入map中
    // (code,(basic_code,table_name,id))
    // (code,(basic_code,id))     (01-1,(A,11))
    val itemMap = new HashMap[String, (String,Long)]()
    while (itemResultSet.next()) {
      itemMap.put(itemResultSet.getString(1), (itemResultSet.getString(2),itemResultSet.getLong(3)))
    }
    itemMap
  }
  
  /**
   * 获取设备编码
   *
   * @param id
   */
  def getItemMaxValue(buildingCode: String = null, collectorCode: String = null, itemCode: String = null) = {
    var ITEM_MAX_VALUE = s"""SELECT
                          |  a."code",
                          |  b."code",
                          |  c."code",
                          |  c."data_type",
                          |  c."max_value"，
                      		|  c."coefficient"
                          |FROM
                          |  ${INFO_NAMESPACE}."tbl_building" a,
                          |  ${INFO_NAMESPACE}."tbl_collector" b,
                          |  ${INFO_NAMESPACE}."tbl_item" c
                          |WHERE
                          |  a."id"=b."building_id"
                          |AND b."id"=c."collector_id" """.stripMargin
    if (buildingCode != null) ITEM_MAX_VALUE = ITEM_MAX_VALUE + s""" AND a."code"=${buildingCode}"""
    if (collectorCode != null) ITEM_MAX_VALUE = ITEM_MAX_VALUE + s""" AND b."code" = ${collectorCode}"""
    if (itemCode != null) ITEM_MAX_VALUE = ITEM_MAX_VALUE + s""" AND c."code" = ${itemCode}"""
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(ITEM_MAX_VALUE)
      resultSet = pres.executeQuery()

    } catch {
      case t: Throwable => t.printStackTrace()
    }
    val maxValueMap = new HashMap[(String,String),(Double,Double)]()
    while (resultSet.next()) {
      var maxValue = resultSet.getDouble(5)
      if(maxValue == null || maxValue < 0){
        maxValue = 10000
		  }
      var coefficient = resultSet.getDouble(6)
      if(coefficient == null || coefficient < 0){
        coefficient = 1
		  }
      maxValueMap.put((s"${resultSet.getString(1)}_${resultSet.getString(2)}_${resultSet.getString(3)}",resultSet.getString(4)), (maxValue,coefficient))
    }
    maxValueMap
  }
  /**
   * 将scala的Array转换为Java的ArrayList
   */
  def toJavaArray(array:Array[JSONArray]) = {
    val javaArray = new ArrayList[JSONArray]
			for(json <- array){
				javaArray.add(json)
			}
    javaArray
  }
  
  /**
   * 使用phoenix写入hbase
   *
   * @param part:DStream的分区
   * @return
   */
  def phoenixWriteHbase(namespace: String, tablename: String, resultArray:Array[JSONArray]) {
    try {
      val dataType = CleaningModule.getColumnsType(namespace + "~" + tablename)
      PhoenixHelper.upsertList(namespace, tablename, toJavaArray(resultArray), dataType);
      log.info(s"表${tablename}写入数据成功，写入数据数量为：${resultArray.size}")
    } catch {
      case t: Throwable =>
        t.printStackTrace() // TODO: handle error
        log.error(s"写入表${tablename}失败！");
    }
  }
  /**
   * 获取tbl_item_current_info表上一次的记录
   */
  def getCurrentInfoMap() = {
    val CURRENT_INFO_TABLE = s"""SELECT
                              |  "item_code",
                              |  TO_CHAR(CONVERT_TZ("date_time", 'GMT', 'Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss'),
                              |  "real_value",
                          		|  "data_type" 
                              |FROM
                              |  ${INFO_NAMESPACE}."${current_info_table}" """.stripMargin
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(CURRENT_INFO_TABLE)
      resultSet = pres.executeQuery()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    SparkFunctions.result2JsonArr(resultSet)
      .map(json => {
        ((json.getString(0),json.getString(3)),(json.getString(1),json.getDouble(2)))
      }).toMap
  }
  
  /**
   * 更新tbl_item_current_info表
   */
  def updateCurrentInfo(jsonArray: ArrayBuffer[JSONArray]){
    val CURRENT_INFO_COUNT_SQL = s"""SELECT MAX("id") FROM ${INFO_NAMESPACE}."tbl_item_current_info" """.stripMargin
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(CURRENT_INFO_COUNT_SQL)
      resultSet = pres.executeQuery()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    var count = 0L
    while(resultSet.next()){
      if(resultSet.getLong(1) != null){
    	  count = resultSet.getLong(1)
      }
    }
    val resultMap = SparkFunctions.result2JsonArr(PhoenixHelper.query(INFO_NAMESPACE, current_info_table, null, null))
      .map(json => {
        ((json.getString(1),json.getString(5)),json.getLong(0))
      }).toMap
    for(i <- 0 until jsonArray.length){
      var id = resultMap.getOrElse((jsonArray(i).getString(0),jsonArray(i).getString(4)), null)
      if(id == null){
        count = count + 1
        id = count
      }
      jsonArray(i).add(0, id)
    }
    phoenixWriteHbase(INFO_NAMESPACE, current_info_table, jsonArray.toArray)
  }
  
  /**
   * 写入data_access表
   */
  def insertDataAccess(jsonArray: ArrayBuffer[JSONArray]){
    val INSERT_DATA_ACCESS = s"""SELECT MAX("id") FROM ${INFO_NAMESPACE}."data_access" """.stripMargin
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(INSERT_DATA_ACCESS)
      resultSet = pres.executeQuery()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    var count = 0L
    while(resultSet.next()){
      if(resultSet.getLong(1) != null){
    	  count = resultSet.getLong(1)
      }
    }
    // 去重
    val distinctJsonArray = jsonArray.distinct
    for(i <- 0 until distinctJsonArray.length){
      count = count + 1
      distinctJsonArray(i).add(0, count.toString())
    }
    phoenixWriteHbase(INFO_NAMESPACE, data_access_table, distinctJsonArray.toArray)
  }
  
  /**
   * 获取data_access表
   * @param time:时间值
   */
  def getDataAccessList(time: String) = {
    val timestamp = time.replaceAll("\\D", "").take(10)
    // data_access 表
    val DATA_ACCESS_SQL = s"""SELECT
                              |  "build_code",
                              |  "collector_code",
                              |  "timestamp" 
                              |FROM
                              |  ${INFO_NAMESPACE}."data_access"
                              |WHERE
                              |  "type"=0
                              |AND
                              |  "timestamp" <= '${timestamp}'
                              |ORDER BY
                              |  "timestamp" """.stripMargin
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(DATA_ACCESS_SQL)
      resultSet = pres.executeQuery()

    } catch {
      case t: Throwable => t.printStackTrace()
    }
    // 对获取到的data_access表数据去重
    SparkFunctions.result2JsonArr(resultSet).distinct
  }
  /**
   * 更新data_access数据,将处理过的数据type置为1
   */
  def updateDataAccess(time: String, maxDataAccessId: Long) = {
    val timestamp = time.replaceAll("\\D", "").take(10)
    // 更新data_access表
    var UPDATE_DATA_ACCESS = s"""UPSERT INTO
                                |  ${INFO_NAMESPACE}."data_access" ("id","type")
                                |SELECT
                            		|  "id",1
                                |FROM
                                |  ${INFO_NAMESPACE}."data_access"
                                |WHERE
                                |  "timestamp" <= '${timestamp}' """.stripMargin
    if(maxDataAccessId != -1) UPDATE_DATA_ACCESS = UPDATE_DATA_ACCESS + s""" AND "id" <= ${maxDataAccessId} """
    var result = 0
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      conn.setAutoCommit(true)
      val pres = conn.prepareStatement(UPDATE_DATA_ACCESS)
      result = pres.executeUpdate()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    result
  }
  
  /**
   * 获取虚拟设备列表
   */
  def getVirtualItemList() = {
    // virtual 虚拟表
    val VIRTUAL_ITEM_SQL = s"""SELECT
                            |  c."code",
                            |  b."code",
                            |  a."code",
                            |  d."formula",
                            |  a."max_value",
                            |  e."code", 
                            |  e."table_name", 
                            |  e."basic_code" 
                            |FROM
                            |  ${INFO_NAMESPACE}."tbl_item" a,
                            |  ${INFO_NAMESPACE}."tbl_collector" b,
                            |  ${INFO_NAMESPACE}."tbl_building" c,
                            |  ${INFO_NAMESPACE}."tbl_item_virtual" d,
                            |  ${INFO_NAMESPACE}."tbl_item_type" e
                            |WHERE
                            |  a."collector_id"= b."id"
                            |AND b."building_id"= c."id"
                            |AND a."id"= d."item_id"
                            |AND a."data_type"= e."id" """.stripMargin
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(VIRTUAL_ITEM_SQL)
      resultSet = pres.executeQuery()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    SparkFunctions.result2JsonArr(resultSet)
  }
  /**
   * 通过itemId获取itemCode
   */
  def getItemCodeById() = {
    // getItemCodeById
    val ITEM_CODE_SQL = s"""SELECT
                          |  a."id",
                          |  c."code",
                          |  b."code",
                          |  a."code"
                          |FROM
                          |  ${INFO_NAMESPACE}."tbl_item" a,
                          |  ${INFO_NAMESPACE}."tbl_collector" b,
                          |  ${INFO_NAMESPACE}."tbl_building" c
                          |WHERE
                          |  a."collector_id"=b."id"
                          |AND b."building_id"=c."id" """.stripMargin
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(ITEM_CODE_SQL)
      resultSet = pres.executeQuery()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    val itemCodeIdMap = new HashMap[Long,String]()
    while (resultSet.next()) {
      itemCodeIdMap.put(resultSet.getLong(1),s"${resultSet.getString(2)}_${resultSet.getString(3)}_${resultSet.getString(4)}")
    }
    itemCodeIdMap
  }
  /**
   * 通过itemCode和datetime查询小时数据
   */
  def getHourDataByCode(tablename: String,itemCode: String, dateTime:String, error: String) = {
    val columns = Array("value","real_value","rate")
    val wheres = Array(s""" "item_name" = '${itemCode}' """,s""" "date_time" = TO_TIMESTAMP('${dateTime}') """,s""" "error" = ${error} """)
    SparkFunctions.result2JsonArr(PhoenixHelper.query(DATA_NAMESPACE, tablename, columns, wheres))
  }
  
  /**
   * 获取分项统计对应map
   */
  def getSubentryMap() = {
    // subentry 分项统计
    val SUBENTRY_SQL = s"""SELECT
                        |  t1."code",
                        |  t2."code",
                        |  t3."code",
                        |  t4."code" 
                        |FROM
                        |  ${INFO_NAMESPACE}."tbl_building" t1
                        |  JOIN ${INFO_NAMESPACE}."tbl_collector" t2 ON t1."id" = t2."building_id"
                        |  JOIN ${INFO_NAMESPACE}."tbl_item" t3 ON t2."id" = t3."collector_id"
                        |  JOIN ${INFO_NAMESPACE}."tbl_subentry" t4 ON t3."subentry" = t4."id" 
                        |WHERE
                        |  t3."data_type"= 11 """.stripMargin
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(INFO_NAMESPACE)
      val pres = conn.prepareStatement(SUBENTRY_SQL)
      resultSet = pres.executeQuery()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    val subentryMap = new HashMap[String,String]
    while(resultSet.next()){
      subentryMap.put(s"${resultSet.getString(1)}_${resultSet.getString(2)}_${resultSet.getString(3)}",resultSet.getString(4))
    }
    subentryMap
  }
  
  /**
   * 获取统计表某段时间的数据
   */
  def getEnergyDataByTime(tablename:String,dateTime: String,endTime:String = null) = {
    var timeCol:String = null
    if(tablename == subentry_hour_table || tablename == subentry_day_table){
      timeCol = "data_time"
    }else {
      timeCol = "date_time"
    }
    var GET_ENERGY_DATA = ""
    if (tablename == elec_day_table || tablename == other_day_table) {
      GET_ENERGY_DATA = GET_ENERGY_DATA + s"""|SELECT
                                              |  "item_name",
                                              |  TO_CHAR(CONVERT_TZ("date_time", 'GMT', 'Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss'),
                                              |  "value",
                                              |  "real_value",
                                              |  "rate",
                                              |  "error",
                                              |  "work_time_value",
                                              |  "other_time_value",
                                              |  "type"
                                              |FROM
                                              |  ${DATA_NAMESPACE}."${tablename}" """.stripMargin
    }else if (tablename == subentry_hour_table || tablename == subentry_day_table) {
      GET_ENERGY_DATA = GET_ENERGY_DATA + s"""|SELECT
                                              |  "building_code",
                                              |  "electricity",
                                              |  "electricity_a",
                                              |  "electricity_b",
                                              |  "electricity_c",
                                              |  "electricity_d",
                                              |  "rate",
                                              |  TO_CHAR(CONVERT_TZ("data_time", 'GMT', 'Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss')
                                              |FROM
                                              |  ${DATA_NAMESPACE}."${tablename}" """.stripMargin
    }else {
      GET_ENERGY_DATA = GET_ENERGY_DATA + s"""|SELECT
                                              |  "item_name",
                                              |  TO_CHAR(CONVERT_TZ("date_time", 'GMT', 'Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss'),
                                              |  "value",
                                              |  "real_value",
                                              |  "rate",
                                              |  "error",
                                              |  "type"
                                              |FROM
                                              |  ${DATA_NAMESPACE}."${tablename}" """.stripMargin
    }
    if(endTime == null){
    	GET_ENERGY_DATA = GET_ENERGY_DATA + s""" WHERE "${timeCol}" = TO_TIMESTAMP('${dateTime}') """
    }else {
		  GET_ENERGY_DATA = GET_ENERGY_DATA + s""" WHERE "${timeCol}" >= TO_TIMESTAMP('${dateTime}') AND "${timeCol}" < TO_TIMESTAMP('${endTime}') """
    }
  	var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(DATA_NAMESPACE)
      val pres = conn.prepareStatement(GET_ENERGY_DATA)
      resultSet = pres.executeQuery()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    SparkFunctions.result2JsonArr(resultSet)
  }
  
  
  
  
}