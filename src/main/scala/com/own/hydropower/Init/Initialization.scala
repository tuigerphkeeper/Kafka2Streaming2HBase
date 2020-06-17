package com.own.hydropower.Init

import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf

/**
 * 初始化
 */
object Initialization {

  /**
   * 初始化SparkConf
   */
  lazy val sc: SparkConf = new SparkConf()
    .setAppName(ReadParameter.STREAM_NAME)
    .setMaster(ReadParameter.STREAM_MASTER)

  /**
   * 初始化zk连接
   *
   * @return zkClient zk客户端
   *         topicDirs topic路径
   */
  def get_zk_conn: (ZkClient, ZKGroupTopicDirs, SimpleConsumer) = {
    val zkClient: ZkClient = new ZkClient(ReadParameter.ZOOKEEPER_QUORUM, Integer.MAX_VALUE, 10000, ZKStringSerializer)
    // 创建一个 ZKGroupTopicDirs 对象，对保存 第一个参数是 kafka broker 的host，第二个是 port
    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(ReadParameter.GROUPS(0), ReadParameter.TOPICS(0))
    // 查看当前topic：__consumer_offsets中已存储的最新的offset
    val simpleConsumer: SimpleConsumer = new SimpleConsumer(ReadParameter.KAFKA_HOST, ReadParameter.KAFKA_PORT.toInt,
      1000000, 64 * 1024, "octServer")
    (zkClient, topicDirs, simpleConsumer)
  }
}
