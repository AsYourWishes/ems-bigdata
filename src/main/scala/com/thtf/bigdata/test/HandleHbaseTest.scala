package com.thtf.bigdata.test

import com.thtf.bigdata.hbase.util.PhoenixHelper
import com.thtf.bigdata.functions.SparkFunctions
import com.thtf.bigdata.functions.PhoenixFunctions
import com.alibaba.fastjson.JSON

object HandleHbaseTest {
  def main(args: Array[String]): Unit = {
    
//    readSomeAndWriteSome()
//    writeSomeErrorData()
//    writeSomeData()
    
    PhoenixFunctions.getItemTypeTable().foreach(println)
    
    
    
  }
  
  /**
   * 把EMS03中历史表的部分数据读取出来，写入到EMS_HISTORY_DATA中的历史表
   */
  def readSomeAndWriteSome(){
    
    val wheres = Array(""" "BuildingID" = '430100A001' """,
                       """ "GateID" = '01' """,
                       """ "MeterID" = '11' """,
                       """ "ParamID" = '01-1' """,
                       """ "Timestamp" >= '20171009120000' """,
                       """ "Timestamp" <= '20171009140000' """)
    val read = SparkFunctions.result2JsonArr(PhoenixHelper.query("EMS03", "DS_HisData", null, wheres))
    read.foreach(println)
    PhoenixFunctions.phoenixWriteHbase("EMS_HISTORY_DATA", "DS_HisData_test_wangyh", read.toArray)
    
    
  }
  
  /**
   * 写入一些错误数据
   */
  def writeSomeErrorData(){
    val errorData1 = """["430100A001","01","10","01-1","01  ",null,"0","48892.60","null","20171009131B00"]"""
		val errorData2 = """["430100A001","01","10","01-1","01  ",null,"0","48892.60","null","null"]"""
		val errorData3 = """["430100A001","01","10","01-1","01  ",null,"0","48,892.60","null","20171009124500"]"""
		val errorData4 = """["430100A001","01","10","01-1","01  ",null,"0",null,"null","20171009125000"]"""
		
		val write = Array(JSON.parseArray(errorData1),
		                  JSON.parseArray(errorData2),
		                  JSON.parseArray(errorData3),
		                  JSON.parseArray(errorData4))
		write.foreach(println)
		PhoenixFunctions.phoenixWriteHbase("EMS_HISTORY_DATA", "DS_HisData_test_wangyh", write)
  }
  
  /**
   * 手动创建一些数据
   */
  def writeSomeData(){
	  val Data1 = """["430100A001","01","10","02-1","01  ",null,"0","48890.80","null","20171009130000"]"""
	  val Data2 = """["430100A001","01","10","02-1","01  ",null,"0","48894.40","null","20171009133000"]"""
	  val Data3 = """["430100A001","01","10","02-1","01  ",null,"0","48898.20","null","20171009140000"]"""
	  val Data4 = """["430100A001","01","10","02-1","01  ",null,"0","48883.40","null","20171009120000"]"""
	  val Data5 = """["430100A001","01","10","02-1","01  ",null,"0","48887.20","null","20171009123000"]"""
	  val Data6 = """["430100A001","01","10","02-1","01  ",null,"0","48892.60","null","20171009131500"]"""
	  val Data7 = """["430100A001","01","10","02-1","01  ",null,"0","48896.20","null","20171009134500"]"""
	  val Data8 = """["430100A001","01","10","02-1","01  ",null,"0","48885.20","null","20171009121500"]"""
	  
	  val write = Array(JSON.parseArray(Data1),
              			  JSON.parseArray(Data2),
              			  JSON.parseArray(Data3),
              			  JSON.parseArray(Data4),
                  	  JSON.parseArray(Data5),
                  	  JSON.parseArray(Data6),
                  	  JSON.parseArray(Data7),
                      JSON.parseArray(Data8))
	  write.foreach(println)
	  PhoenixFunctions.phoenixWriteHbase("EMS_HISTORY_DATA", "DS_HisData_test_wangyh", write)
  }
  
  
  
  
  
  
  
  
  
}