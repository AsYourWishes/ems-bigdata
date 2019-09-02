package com.thtf.bigdata.test

import java.sql.DriverManager

object MyJDBCTest {
  def main(args: Array[String]): Unit = {
    jdbcTest
  }
  
  
  def jdbcTest(){
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_test_db01","root","root")
    val stmt = conn.createStatement()
    val sql = "select MAX(id) from user_copy1"
    val rs = stmt.executeQuery(sql)
    if(rs.next()){
      val id = rs.getLong(1)
      println(id)
      println(id == null)
    }
  }
  
  
  
}