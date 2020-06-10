package com.own.hydropower.common_params

import org.apache.log4j.LogManager
import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import java.io.{FileInputStream, IOException}

object Comm_Params {

  private val log = LogManager.getLogger(Comm_Params.getClass)

  var hdfsPath: String = _
  var zookeeper: String = _ // kafkaZookeeper集群地址
  var topic: String = _ // kafkaTopic
  var group: String = _ // group_1
  var brokers: String = _ // Kafka节点
  var sparkStreamMaster: String = _ // stream运行模式
  var sparkStreamName: String = _ // stream任务名称
  var sparkSqlMaster: String = _ // sql运行模式
  var sparkSqlName: String = _
  var hbaseQuorum: String = _ // HBase_IP
  var hbasePort: String = _ // HBase端口
  var hbasePath: String = _ // HBase在zk上的路径
  var kafka_hosts: String = _ // kafka_leader
  var kafka_port: String = _
  var tableName: String = _
  var family: String = _
  var column1: String = _
  var column2: String = _
  var column3: String = _
  var column4: String = _
  var column5: String = _

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
      PropertyConfigurator.configure("/usr/local/projects/sparkstreaming/log4j.properties")
      pp.load(new FileInputStream("/usr/local/projects/sparkstreaming/comm_params.properties"))

//      PropertyConfigurator.configure("src\\main\\resources\\log4j.properties")
//      pp.load(new FileInputStream("src\\main\\resources\\comm_params.properties"))
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
