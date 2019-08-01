package com.thtf.bigdata.common

/**
 * 存放properties的key
 */
object PropertiesConstant {

  // kafka
  val KAFKA_BROKERS = "kafka.brokers"
  val KAFKA_TOPICS = "kafka.topics"
  val KAFKA_GROUPID_SAVEHISDATA = "kafka.groupId.saveHisData"
  val KAFKA_GROUPID_STREAMPROCESSING = "kafka.groupId.streamProcessing"
  val KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset"
  
  // zookeeper
  val ZOOKEEPER_URL = "zookeeper.url"

  // spark
  val SPARK_APPNAME_KAFKA = "spark.appName.kafka"
  val SPARK_APPNAME_HISDATA = "spark.appName.hisdata"
  val SPARK_MASTER = "spark.master"
  val SPARK_BATCH = "spark.batchInterval"
  val SPARK_MAXRATEPERPARTITION = "spark.streaming.kafka.maxRatePerPartition"
  val SPARK_PARALLELISM = "spark.default.parallelism"
  val SPARK_REPARTITION = "spark.repartition"
  val SPARK_CHECKPOINT = "spark.checkpoint"
  val SPARK_WINDOWDURATION = "spark.windowDuration"
  val SPARK_SLIDEDURATION = "spark.slideDuration"
  val SPARK_CHECKINTERVALMILLIS = "spark.checkIntervalMillis"
  val SPARK_SAVEERRORDATA = "spark.saveErrorData"
  // error data time
  val SPARK_ERRORDATA_FROMTIME = "spark.errorData.fromTime"
  val SPARK_ERRORDATA_ENDTIME = "spark.errorData.endTime"
  
  /**
   * 是否处理补传数据
   */
  val SPARK_SUPPLEMENT = "spark.supplement"

  // phoenix
  /**
   * phoenix预提交数量
   */
  val PHOENIX_COMMITSIZE = "phoenix.prepare.commit.size"
  // dbtools
  val PHOENIX_DRIVER = "phoenix.driver"
  val PHOENIX_URL = "phoenix.url"

  // hbase
  val HBASE_URL = "hbase.url"
  val HBASE_ZNODE = "hbase.znode"
  // public table
  val HBASE_NAMESPACE = "hbase.namespace"
  val HBASE_TABLENAME = "hbase.tablename"
  val HBASE_COLUMNFAMILY = "hbase.columnfamily"
  val HBASE_COLUMNNAME = "hbase.columnname"
  val HBASE_PUBTABLE = "hbase.pubtable"
  val HBASE_PARTITIONCOUNT = "hbase.partitionCount"

}