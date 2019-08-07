package com.thtf.bigdata.functions

import com.thtf.bigdata.entity.DateTimeEntity
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.Logger
import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONArray

/**
 * 封装一些操作spark RDD的方法
 */
object SparkFunctions {

  val log = Logger.getLogger(this.getClass)

  val HISDATA_DATAFORMAT = "yyyyMMddHHmmss"
  val ENERGY_DATAFORMAT = "yyyy-MM-dd HH:mm:ss"

  /**
   * 根据当前时间获取所有需要的时间
   *
   * @param timeUnit: 时间单位
   * @param time:默认为当前时间，可以传入一个时间值
   * @return
   */
  def getAllTime(time: String = null) = {
    val date = DateTimeEntity
    val simpleDateFormat = new SimpleDateFormat(ENERGY_DATAFORMAT)
    val calendar = Calendar.getInstance();
    if (time != null) {
      if (time.length() == 14) calendar.setTime(new SimpleDateFormat(HISDATA_DATAFORMAT).parse(time))
      if (time.length() == 19) calendar.setTime(new SimpleDateFormat(ENERGY_DATAFORMAT).parse(time))
    }
    // 将时间秒置为0
    calendar.add(Calendar.SECOND, -calendar.get(Calendar.SECOND))
    val currentMinuteTime = simpleDateFormat.format(calendar.getTime)
    // 将时间的分钟置为0
    calendar.add(Calendar.MINUTE, -calendar.get(Calendar.MINUTE))
    val currentHourTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.HOUR_OF_DAY, -2)
    val lastTwoHourTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.HOUR_OF_DAY, 3)
    val nextHourTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.HOUR_OF_DAY, -2)
    val lastHourTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.HOUR_OF_DAY, 1)
    // 将小时置为0
    calendar.add(Calendar.HOUR_OF_DAY, -calendar.get(Calendar.HOUR_OF_DAY))
    val currentDayTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.DAY_OF_MONTH, 1)
    val nextDayTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.DAY_OF_MONTH, -2)
    val lastDayTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.DAY_OF_MONTH, 1)
    // 将天置为1号
    calendar.add(Calendar.DAY_OF_MONTH, -(calendar.get(Calendar.DAY_OF_MONTH) - 1))
    val currentMonthTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.MONTH, 1)
    val nextMonthTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.MONTH, -2)
    val lastMonthTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.MONTH, 1)
    date.apply(currentMinuteTime,currentHourTime, lastHourTime, lastTwoHourTime, nextHourTime, currentDayTime, lastDayTime, nextDayTime, currentMonthTime, lastMonthTime, nextMonthTime)
  }

  /**
   * 检查配置的时间是否可以转换为时间类型
   * 重新处理历史数据时使用
   */
  def checkStringTime(fromTime: String, endTime: String) = {
    var checkout = true
    val simple14 = new SimpleDateFormat(HISDATA_DATAFORMAT)
    val simple19 = new SimpleDateFormat(ENERGY_DATAFORMAT)
    try {
      // 测试字符串是否可以转换为时间
      if(fromTime.trim().length() == 14){
    	  simple14.parse(fromTime)
      }else {
        simple19.parse(fromTime)
      }
      if(endTime.trim().length() == 14){
    	  simple14.parse(endTime)
      }else {
        simple19.parse(endTime)
      }
      if (endTime <= fromTime) {
        checkout = false
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace() // TODO: handle error
        log.error("需要处理数据的起止时间不正确，请重新配置！！")
        checkout = false
    }
    checkout
  }
  /**
   * 检查传入的字符串是否可以转换为时间类型
   * 清洗数据时使用
   */
  def checkStringTime(time: String) = {
    var checkout = true
    val simple14 = new SimpleDateFormat(HISDATA_DATAFORMAT)
    val simple19 = new SimpleDateFormat(ENERGY_DATAFORMAT)
    try {
      // 测试字符串是否可以转换为时间
      if(time.trim().length() == 10){
        simple14.parse(time.trim() + "0000")
      }else if(time.trim().length() == 14){
    	  simple14.parse(time)
      }else {
        simple19.parse(time)
      }
    } catch {
      case t: Throwable =>
        checkout = false
    }
    checkout
  }
  
  /**
   * 将通过检查的字符串转换为timestamp
   */
  def stringToTimeStamp(time:String) = {
    val simple14 = new SimpleDateFormat(HISDATA_DATAFORMAT)
    val simple19 = new SimpleDateFormat(ENERGY_DATAFORMAT)
    if(time.trim().length() == 10){
      simple14.parse(time.trim() + "0000").getTime
    }else if(time.trim().length() == 14){
      simple14.parse(time).getTime
    }else {
      simple19.parse(time).getTime
    }
  }
  
  /**
   * 检查传入的字符串是否可以转换为Double类型
   * 只有DS_HisData表有用(只有phoenix中以varchar形式存的数值才需要检验)
   */
  def checkStringNumber(num: String) = {
    var checkout = true
    try {
      num.toDouble
    } catch {
      case t: Throwable =>
        checkout = false
    }
    checkout
  }
  /**
   * 计算两个字符串类型的数值的差值
   */
  def getSubtraction(smaller: String,bigger: String) = {
    (BigDecimal(bigger) - BigDecimal(smaller)).toDouble
  }
  /**
   * 计算两个字符串类型的数值的差值
   */
  def getSubtraction(smaller: Double,bigger: Double) = {
		  (BigDecimal(bigger) - BigDecimal(smaller)).toDouble
  }
  /**
   * 计算两个字符串类型的数值的差值
   */
  def getSubtraction(smaller: Integer,bigger: Integer) = {
		  (BigDecimal(bigger) - BigDecimal(smaller)).toDouble
  }
  /**
   * 计算两个字符串类型的数值的和
   */
  def getSum(smaller: String,bigger: String) = {
		  (BigDecimal(bigger) + BigDecimal(smaller)).toDouble
  }
  /**
   * 计算两个字符串类型的数值的和
   */
  def getSum(smaller: Double,bigger: Double) = {
		  (BigDecimal(bigger) + BigDecimal(smaller)).toDouble
  }
  /**
   * 计算两个字符串类型的数值的和
   */
  def getSum(smaller: Integer,bigger: Integer) = {
		  (BigDecimal(bigger) + BigDecimal(smaller)).toDouble
  }
  /**
   * 计算两个字符串类型的数值的乘积
   */
  def getProduct(smaller: String,bigger: String) = {
		  (BigDecimal(bigger) * BigDecimal(smaller)).toDouble
  }
  /**
   * 计算两个Double类型的数值的乘积
   */
  def getProduct(smaller: Double,bigger: Double) = {
		  (BigDecimal(bigger) * BigDecimal(smaller)).toDouble
  }
  
  /**
   * 将输入变量转换为JSON数组形式
   *
   * @param stringArr：String类型的变量
   * @return
   */
  def strArr2JsonArr(stringArr: String*) = {
    val jsonArr = new JSONArray
    for(str <- stringArr){
      jsonArr.add(str)
    }
    jsonArr
  }
  
  /**
   * 将查询到的resultSet转换为JSONArray
   *
   * @param resultSet：查询到的结果集
   * @return
   */
  def result2JsonArr(resultSet: ResultSet) = {
    val resultArray: ArrayBuffer[JSONArray] = ArrayBuffer()
    if (resultSet != null) {
      val columnCount = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        val jsonArr = new JSONArray
        for (index <- 1 to columnCount) {
          jsonArr.add(resultSet.getString(index))
        }
        resultArray.append(jsonArr)
      }
    }
    resultArray
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

}