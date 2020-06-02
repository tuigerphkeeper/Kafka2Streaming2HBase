package com.own.hydropower.sql

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.LogManager
import java.io.FileInputStream
import java.io.IOException
import java.util.Properties

object Collection {

  private val log = LogManager.getLogger("Collocation")

  var hive_dir: String = null //hive????��??
  var toHdfsRoot: String = null //??????hdfs??sql
  var toHdfsTable: String = null
  var businessType: String = null
  var sql_toHiveTable: String = null
  var sql_toHiveFromTable: String = null

  /**
   * ????????��???????
   */
  def initConfig() {
    log.info("????????????...")
    BasicConfigurator.configure()
    readCommons()
  }

  /**
   * ???sparkSQL.properties??????????
   */
  private def readCommons() {
    val pp = new Properties
    try {
      log.info("???log4j.properties...")
      PropertyConfigurator.configure("/usr/kbd/toHdfs/log4j.properties")
      log.info("???sparkSQL.properties???????...")
      pp.load(new FileInputStream("/usr/kbd/toHdfs/sparkSQL.properties"))
    } catch {
      case e: IOException =>
        log.error(e)
    }
    if (pp != null) {
      hive_dir = pp.getProperty("hive_dir")
      toHdfsRoot = pp.getProperty("tohdfsroot")
      toHdfsTable = pp.getProperty("tohdfstable")
      businessType = pp.getProperty("BusinessType")
      sql_toHiveTable = pp.getProperty("sql_toHiveTable")
      sql_toHiveFromTable = pp.getProperty("sql_toHiveFromTable")
    }
    else log.error("???sparkSQL.xml??????????????")
  }
}
