package com.thtf.bigdata.entity

object Entities {
}

/**
 * 历史数据
 */
case class HisDataEntity(
  buildingID: String,
  gateID: String,
  meterID: String,
  paramID: String,
  typeOf: String,
  name: String,
  status: String,
  value: String,
  unit: String,
  timestamp: String)
/**
 * 时间值
 */
case class DateTimeEntity(
	val currentMinuteTime: String,
  val currentHourTime: String,
  val lastHourTime: String,
  val lastTwoHourTime: String,
  val nextHourTime: String,
  val currentDayTime: String,
  val lastDayTime: String,
  val nextDayTime: String,
  val currentMonthTime: String,
  val lastMonthTime: String,
  val nextMonthTime: String)