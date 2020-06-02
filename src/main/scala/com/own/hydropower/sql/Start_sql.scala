package com.own.hydropower.sql

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession


object Start_sql {
  private val log = LogManager.getLogger("Main")
  //获取配置信息

  def main(args: Array[String]): Unit = {
    Collection.initConfig()
    log.info("加载sparkSQL配置...")
    val sparkSession: SparkSession = SparkSession.builder()
      //     .master("local[2]")
      .appName("sparkSQL")
      .config("spark.sql.warehouse.dir", Collection.hive_dir)
      .enableHiveSupport()
      .getOrCreate()
    dataToHdfs(sparkSession)
    dataToHive(sparkSession)
  }

  def dataToHdfs(sparkSession: SparkSession): Unit = {
    val time1 = System.currentTimeMillis()
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(now)
    log.info("开始数据导出到hdfs...")
    val rootpath = Collection.toHdfsRoot
    val sql = "insert overwrite directory " + "'" + rootpath + Collection.businessType + date + "/" + "'" + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' select * from " + Collection.toHdfsTable
    println("tohdfs:  " + sql)
    sparkSession.sql(sql)
    val cmd = "hadoop fs -rm -f " + rootpath + Collection.businessType + date + "/_SUCCESS"
    println("cmd: " + cmd)
    Runtime.getRuntime.exec(cmd)
    log.info("导出结束...，共耗时:" + (System.currentTimeMillis() - time1) / 1000 + "秒。")
  }

  def dataToHive(sparkSession: SparkSession): Unit = {
    log.info("开始数据查询...")
    val sql = "insert overwrite table " + Collection.sql_toHiveTable + " select * from " + Collection.sql_toHiveFromTable
    print("insert  " + sql + "----")
    sparkSession.sql(sql)
    log.info("导入到hive结束...")
  }
}
