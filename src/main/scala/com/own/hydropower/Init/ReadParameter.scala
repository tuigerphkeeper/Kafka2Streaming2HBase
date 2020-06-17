package com.own.hydropower.Init

import java.io.{FileInputStream, IOException}
import java.util.Properties

import org.apache.log4j.{LogManager, PropertyConfigurator}

/**
 * 读取properties文件参数
 */
object ReadParameter extends Serializable {
  @transient private val log = LogManager.getLogger(ReadParameter.getClass)

  var ZOOKEEPER_QUORUM: String = _ // Zookeeper集群地址
  var TOPICS: Array[String] = _ // kafka topics
  var GROUPS: Array[String] = _ // kafka group
  var BROKERS: String = _ // Kafka节点
  var STREAM_MASTER: String = _ // stream运行模式
  var STREAM_NAME: String = _ // stream任务名称
  var HBASE_PORT: String = _ // HBase端口
  var HBASE_PATH: String = _ // HBase在zk上的路径
  var KAFKA_HOST: String = _ // kafka_leader
  var KAFKA_PORT: String = _ // kafka端口
  var TABLE_NAMES: Array[String] = _ // hbase表
  var FAMILIES: Array[String] = _ // 列簇
  var COLUMNS: Array[String] = _ // 列

  /**
   * 读取commons.properties
   */
  private def readCommons() {
    val pp = new Properties()
    try {
      PropertyConfigurator.configure("src\\main\\resources\\log4j.properties")
      pp.load(new FileInputStream("src\\main\\resources\\comm_params.properties"))
      //      PropertyConfigurator.configure("/usr/local/projects/sparkstreaming/log4j.properties")
      //      pp.load(new FileInputStream("/usr/local/projects/sparkstreaming/comm_params.properties"))
    } catch {
      case e: IOException =>
        log.error(e)
    }
    if (pp != null) {
      ZOOKEEPER_QUORUM = pp.getProperty("zk_quorum")
      BROKERS = pp.getProperty("brokers")
      TOPICS = pp.getProperty("topics").split(",")
      GROUPS = pp.getProperty("groups").split(",")
      STREAM_MASTER = pp.getProperty("stream_master")
      STREAM_NAME = pp.getProperty("stream_name")
      KAFKA_HOST = pp.getProperty("kafka_hosts")
      KAFKA_PORT = pp.getProperty("kafka_port")
      HBASE_PORT = pp.getProperty("zk_port")
      HBASE_PATH = pp.getProperty("hbase_path")
      TABLE_NAMES = pp.getProperty("table_names").split(",")
      FAMILIES = pp.getProperty("families").split(",")
      COLUMNS = pp.getProperty("columns").split(",")
    }
    else log.warn("comm_params空文件")
  }

  log.info("初始化comm_params.properties文件")
  readCommons()
}