package com.own.hydropower.data2hbase

import com.own.hydropower.Init.{Initialization, ReadParameter}
import kafka.api.{OffsetFetchRequest, TopicMetadata, TopicMetadataRequest, TopicMetadataResponse}
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}
import kafka.utils.ZkUtils
import org.apache.log4j.LogManager
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

class Manage_offset {
  @transient lazy private val log = LogManager.getLogger(classOf[Manage_offset])
  /**
   * 获取kafka的offset
   *
   * @return offset
   */
  def get_current_offset(): Map[TopicAndPartition, Long] = {
    // 定义一个topic请求，为了获取相关topic的信息（不包括offset,有partition）
    val topicReq: TopicMetadataRequest = new TopicMetadataRequest(Seq(ReadParameter.TOPICS(0)), 0)
    // 发送请求，得到kafka响应
    val res: TopicMetadataResponse = Initialization.get_zk_conn._3.send(topicReq)
    val topicMetaOption: Option[TopicMetadata] = res.topicsMetadata.headOption
    // 定义一个TopicAndPartition的格式，便于后面请求offset
    val topicAndPartition: Seq[TopicAndPartition] = topicMetaOption match {
      case Some(tm) => tm.partitionsMetadata.map(pm => TopicAndPartition(ReadParameter.TOPICS(0), pm.partitionId))
      case None => Seq[TopicAndPartition]()
    }
    // 定义一个请求，传递的参数为groupId,topic,partitionId,这三个也正好能确定对应的offset的位置
    val fetchRequest: OffsetFetchRequest = OffsetFetchRequest(ReadParameter.TOPICS(0), topicAndPartition)
    // 向kafka发送请求并获取返回的offset信息
    val fetchResponse: Map[TopicAndPartition, OffsetMetadataAndError] = Initialization.get_zk_conn._3.fetchOffsets(fetchRequest).requestInfo
    val offsetList = fetchResponse.map { l =>
      val part_name = l._1.partition
      val offset_name = l._2.offset
      (ReadParameter.TOPICS(0), part_name, offset_name)
    }.toList
    // 把offsetList 转成Map[TopicAndPartition, Long]格式
    val fromOffsets: Map[TopicAndPartition, Long] = transform_offset(offsetList)
    fromOffsets
  }

  /**
   * 获取到的各个节点的offset类型由 List[(String, Int, Long)转成Map[TopicAndPartition, Long]
   *
   * @param list 各个节点offset集合
   * @return 返回转换完成后的Map[TopicAndPartition, Long]类型offset
   */
  def transform_offset(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1, offset._2)
      fromOffsets += (tp -> offset._3)
    }
    fromOffsets
  }


  /**
   * 读取kafka的offset存在zk上
   *
   * @param kafkaStream kafka消费者接收到的数据
   */
  def save_offset(kafkaStream: InputDStream[(String, String)]) {
    var offsetRanges = Array[OffsetRange]()
    log.info("开始存储本批次offset至zk......")
    kafkaStream.transform(rdd => {
      // 得到该 rdd 对应 kafka 的消息的 offset
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(msg => msg._2).foreachRDD { _ =>
      for (o <- offsetRanges) {
        //(o.partition)
        // 获取 zookeeper 中offset 的路径
        val zkPath = s"${Initialization.get_zk_conn._2.consumerOffsetDir}/${o.partition}"
        // 将该 partition 的 offset 保存到 zookeeper
        ZkUtils.updatePersistentPath(Initialization.get_zk_conn._1, zkPath, o.fromOffset.toString)
      }
    }
    log.info("存储本批次offset至zk完毕")
  }
}
