package com.own.hydropower.common_params

import org.apache.log4j.LogManager
import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import java.io.{FileInputStream, IOException}

object Comm_Params {

  private val log = LogManager.getLogger(Comm_Params.getClass)

  var hdfsPath: String = null
  var zookeeper: String = null // kafkaZookeeper集群地址
  var topic: String = null // kafkaTopic
  var group: String = null // group_1
  var brokers: String = null // Kafka节点
  var sparkStreamMaster: String = null // stream运行模式
  var sparkStreamName: String = null // stream任务名称
  var sparkSqlMaster: String = null // sql运行模式
  var sparkSqlName: String = null
  var hbaseQuorum: String = null // HBase_IP
  var hbasePort: String = null // HBase端口
  var hbasePath: String = null // HBase在zk上的路径
  var kafka_hosts: String = null // kafka_leader
  var kafka_port: String = null
  var tableName: String = null
  var family: String = null
  var column1: String = null
  var column2: String = null
  var column3: String = null
  var column4: String = null
  var column5: String = null

  /**
   * 初始化所有的通用信息
   */
  def initConfig() {
    log.info("初始化配置信息")
    readCommons()
  }

  /**
   * 读取commons.properties，加载到内存
   */
  private def readCommons() {
    val pp = new Properties()
    try {
      PropertyConfigurator.configure(
        "D:\\IdeaProjects\\BigData_Spark\\src\\main\\resources\\\\log4j.properties");
      pp.load(new FileInputStream(
        "D:\\IdeaProjects\\BigData_Spark\\src\\main\\resources\\comm_params.properties"));
    } catch {
      case e: IOException =>
        log.error(e)
    }
    if (pp != null) {
      hdfsPath = pp.getProperty("hdfsPath")
      zookeeper = pp.getProperty("zookeeper")
      brokers = pp.getProperty("brokers")
      topic = pp.getProperty("topic")
      group = pp.getProperty("group")
      sparkStreamMaster = pp.getProperty("streamMaster")
      sparkStreamName = pp.getProperty("streamName")
      sparkSqlMaster = pp.getProperty("sqlMaster")
      sparkSqlName = pp.getProperty("sqlName")
      hbaseQuorum = pp.getProperty("quorum")
      kafka_hosts = pp.getProperty("kafka_hosts")
      kafka_port = pp.getProperty("kafka_port")
      hbasePort = pp.getProperty("port")
      hbasePath = pp.getProperty("hbasePath")
      tableName = pp.getProperty("tableName")
      family = pp.getProperty("family")
      column1 = pp.getProperty("column1")
      column2 = pp.getProperty("column2")
      column3 = pp.getProperty("column3")
      column4 = pp.getProperty("column4")
      column5 = pp.getProperty("column5")
    }
    else log.error("commons.xml配置文件读取错误")
  }
}
