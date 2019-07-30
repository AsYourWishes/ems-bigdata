package com.thtf.bigdata.common

/**
 * 表名
 */
object TableConstant {

  // 命名空间
  val NAMESPACE = "phoenix.namespace"
  // 历史数据表名
  val HISDATA_TABLE = "phoenix.hisdata"
  // 错误历史数据表名
  val HISDATA_TABLE_ERROR = "phoenix.hisdata.error"
  // 电      小时表
  val ELECTRICITY_HOUR = "phoenix.elec.hour"
  // other小时表
  val OTHER_HOUR = "phoenix.other.hour"
  // 电         天表
  val ELECTRICITY_DAY = "phoenix.elec.day"
  // other 天表
  val OTHER_DAY = "phoenix.other.day"
  // 电        月表
  val ELECTRICITY_MONTH = "phoenix.elec.month"
  // other 月表
  val OTHER_MONTH = "phoenix.other.month"
  // 分项 小时表
  val SUBENTRY_HOUR = "phoenix.elec.subentry.hour"
  // 分项     天表
  val SUBENTRY_DAY = "phoenix.elec.subentry.day"
  // current_info
  val CURRENT_INFO = "phoenix.currentInfo"
  // data_access
  val DATA_ACCESS = "phoenix.data_access"
  // tbl_item_type
//  val ITEM_TYPE = "phoenix.itemType"

}