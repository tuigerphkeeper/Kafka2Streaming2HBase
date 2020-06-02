package com.own.hydropower.data2dbs

import java.io.FileInputStream
import java.util.Properties

import com.own.hydropower.common_params.Comm_Params
import kafka.api._
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 解析原始数据类型
 * 从kafka获取数据
 * 利用MR程序将数据导入HBase
 */
class DataFromKafka() {

  Comm_Params.initConfig()
  @transient lazy val log = LogManager.getLogger(classOf[DataFromKafka])

  def getConsumptionData(): Unit = {
    //spark配置信息
    log.info("加载spark配置...")
    val conf: SparkConf = new SparkConf()
      .setMaster(Comm_Params.sparkStreamMaster)
      .setAppName(Comm_Params.sparkStreamName)
    val pp = new Properties
    pp.load(new FileInputStream("/usr/kbd/toHbase/commons.properties"))
    //pp.load(new FileInputStream("E:\\IdeaProject\\YCdata\\src\\main\\resources\\commons.properties"))
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val broadcast: Broadcast[Properties] = ssc.sparkContext.broadcast(pp) //广播变量
    //"auto.offset.reset" -> "smallest" //smallest表示从开始位置消费所有消息
    var kafkaParams: Map[String, String] = Map("metadata.broker.list" -> Comm_Params.brokers, "group.id" -> Comm_Params.group)
    //查看当前topic：__consumer_offsets中已存储的最新的offset
    val simpleConsumer: SimpleConsumer = new SimpleConsumer(Comm_Params.kafka_hosts, Comm_Params.kafka_port.toInt, 1000000, 64 * 1024, "octServer")
    //定义一个topic请求，为了获取相关topic的信息（不包括offset,有partition）
    val topicReq: TopicMetadataRequest = new TopicMetadataRequest(Seq(Comm_Params.topic), 0)
    log.info("发送kafka请求获取topic信息:" + topicReq)
    //发送请求，得到kafka响应
    val res: TopicMetadataResponse = simpleConsumer.send(topicReq)
    val topicMetaOption: Option[TopicMetadata] = res.topicsMetadata.headOption
    //定义一个TopicAndPartition的格式，便于后面请求offset
    val topicAndPartition: Seq[TopicAndPartition] = topicMetaOption match {
      case Some(tm) => tm.partitionsMetadata.map(pm => TopicAndPartition(Comm_Params.topic, pm.partitionId))
      case None => Seq[TopicAndPartition]()
    }
    //定义一个请求，传递的参数为groupId,topic,partitionId,这三个也正好能确定对应的offset的位置
    val fetchRequest: OffsetFetchRequest = OffsetFetchRequest(Comm_Params.topic, topicAndPartition)
    //向kafka发送请求并获取返回的offset信息
    val fetchResponse: Map[TopicAndPartition, OffsetMetadataAndError] = simpleConsumer.fetchOffsets(fetchRequest).requestInfo
    val offsetList = fetchResponse.map { l =>
      val part_name = l._1.partition
      val offset_name = l._2.offset
      (Comm_Params.topic, part_name, offset_name)
    }.toList
    //把offsetList 转成Map[TopicAndPartition, Long]格式
    var fromOffsets: Map[TopicAndPartition, Long] = setFromOffsets(offsetList)
    //创建一个 ZKGroupTopicDirs 对象，对保存 第一个参数是 kafka broker 的host，第二个是 port
    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(Comm_Params.group, Comm_Params.topic)
    //zookeeper 的host 和 ip，创建一个 client
    //val zkClient = new ZKClient(zkHosts,Integer.MAX_VALUE,10000,ZKStringSerializer)
    log.info("发送zkClient请求...")
    val zkClient: ZkClient = new ZkClient(Comm_Params.zookeeper, Integer.MAX_VALUE, 10000, ZKStringSerializer)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    val children: Int = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    println(s"${topicDirs.consumerOffsetDir}")
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    //获取offset 创建kafkaStream: InputDStream
    var kafkaStream: InputDStream[(String, String)] = null
    if (children > 0) {
      //如果保存过 offset，这里更好的做法，还应该和kafka上最小的offset做对比，不然会报OutOfRange的错误
      for (i <- 0 until children) {
        //获取zk上的存储的offset
        val partitionOffset: String = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        log.info(s"${topicDirs.consumerOffsetDir}/${i}" + "--" + partitionOffset)
        var partOffset = partitionOffset.toLong
        val tp: TopicAndPartition = TopicAndPartition(Comm_Params.topic, i)
        //获取kafka最小的offset
        val earliestOffset: OffsetRequest = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        //获取kafka最大的offset
        val latestOffset: OffsetRequest = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
        val earOffset: Seq[Long] = simpleConsumer.getOffsetsBefore(earliestOffset).partitionErrorAndOffsets(tp).offsets
        val latOffset: Seq[Long] = simpleConsumer.getOffsetsBefore(latestOffset).partitionErrorAndOffsets(tp).offsets
        // 通过比较从 kafka 上该 partition 的最小 offset ,当前offset和 zk 上保存的 offset，进行选择
        log.info(earOffset + "------" + latOffset + "------" + partOffset)
        if (earOffset.length > 0 && partitionOffset.toLong < earOffset.head) {
          partOffset = earOffset.head
        }
        if (latOffset.length > 0 && partitionOffset.toLong > latOffset.head) {
          partOffset = 0
        }
        fromOffsets += (tp -> partOffset)
      }
      log.info("------------------- createDirectStream from zk offset -------")
      //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
      val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.key(), mam.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      log.info("------------------- chilern <=0 createDirectStream -------")
      kafkaParams += ("auto.offset.reset" -> "smallest")
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(Comm_Params.topic))
    }
    //new DataWriteDatabase().writeDatabase(kafkaStream,pp)
//    new DataWriteDatabase(broadcast).writeDatabase(kafkaStream)

    println("初始化kafka完毕。刷新offset")
    dataLoseProcessing(kafkaStream, topicDirs, zkClient)
    //启动程序
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 读取kafka的offset存在zk上
   *
   * @param kafkaStream kafka消费者接收到的数据
   * @param topicDirs   topic在zk上的路径
   * @param zkClient    zk客户端
   */
  def dataLoseProcessing(kafkaStream: InputDStream[(String, String)], topicDirs: ZKGroupTopicDirs, zkClient: ZkClient) {
    log.info("开始更新offset...")
    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform(rdd => {
      //得到该 rdd 对应 kafka 的消息的 offset
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(msg => msg._2).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        //(o.partition)
        // 获取 zookeeper 中offset 的路径
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
      }
    }
    log.info("offset更新完毕...")
  }

  /**
   * 获取到的各个节点的offset类型由 List[(String, Int, Long)转成Map[TopicAndPartition, Long]
   *
   * @param list 各个节点offset集合
   * @return 返回转换完成后的Map[TopicAndPartition, Long]类型offset
   */
  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1, offset._2)
      fromOffsets += (tp -> offset._3)
    }
    fromOffsets
  }
}
