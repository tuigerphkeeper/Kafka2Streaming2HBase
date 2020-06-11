package com.own.hydropower.sql

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.LogManager
import java.io.FileInputStream
import java.io.IOException
import java.util.Properties

object Collection {

  private val log = LogManager.getLogger("Collocation")

  var hive_dir: String = _
  var hdfsPath: String = _
  var hdfsTable: String = _
  var businessType: String = _
  var sql_toHiveTable: String = _
  var sql_toHiveFromTable: String = _

  /**
   * 初始化信息
   */
  def initConfig() {
    log.info("初始化配置信息")
    BasicConfigurator.configure()
    readCommons()
  }

  /**
   * 读取spark_sql.properties，加载到内存
   */
  private def readCommons() {
    val pp = new Properties()
    try {
      //      PropertyConfigurator.configure("/usr/kbd/toHdfs/log4j.properties")
      //      pp.load(new FileInputStream("/usr/kbd/toHdfs/sparkSQL.properties"))
      PropertyConfigurator.configure("src\\main\\resources\\log4j.properties")
      pp.load(new FileInputStream("src\\main\\resources\\spark_sql.properties"))
    } catch {
      case e: IOException =>
        log.error(e)
    }
    if (pp != null) {
      hive_dir = pp.getProperty("hive_dir")
      hdfsPath = pp.getProperty("hdfsPath")
      hdfsTable = pp.getProperty("hdfsTable")
      businessType = pp.getProperty("BusinessType")
      sql_toHiveTable = pp.getProperty("sql_toHiveTable")
      sql_toHiveFromTable = pp.getProperty("sql_toHiveFromTable")
    }
    else log.error("spark_sql.properties配置文件读取错误")
  }
}
