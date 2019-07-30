package com.thtf.bigdata.hbase.util

import java.sql.ResultSet
import com.thtf.bigdata.util.PropertiesUtils
import com.thtf.bigdata.common.TableConstant

object ExportTableInfoUtils {

  def main(args: Array[String]): Unit = {

    val namespace = PropertiesUtils.getPropertiesByKey(TableConstant.NAMESPACE)
    val tablename = PropertiesUtils.getPropertiesByKey(TableConstant.ELECTRICITY_MONTH)
    val sql = """

""".stripMargin

    val re1 = createTable(namespace, sql)
    val re2 = writeTableInfo(namespace, tablename)

    println(re1)
    println(re2)

  }

  def createTable(namespace: String, sql: String) = {
    var resultSet: ResultSet = null
    try {
      val conn = PhoenixHelper.getConnection(namespace)
      val pres = conn.prepareStatement(sql)
      resultSet = pres.executeQuery()

    } catch {
      case t: Throwable => t.printStackTrace()
    }
    resultSet
  }

  def writeTableInfo(namespace: String, tablename: String) = {
    val row = namespace + "~" + tablename
    val cols = ""
    var sql = ""
    if (tablename == null || tablename == "") {
      null
    } else {
      if (namespace == null || namespace == "") {
        sql = s"""UPSERT INTO "${tablename}" VALUES ('${row}','${cols}')"""
      } else {
        sql = s"""UPSERT INTO ${namespace}."${tablename}" VALUES ('${row}','${cols}')"""
      }
      var resultSet: ResultSet = null
      try {
        val conn = PhoenixHelper.getConnection(namespace)
        val pres = conn.prepareStatement(sql)
        resultSet = pres.executeQuery()

      } catch {
        case t: Throwable => t.printStackTrace()
      }
      resultSet
    }

  }

}