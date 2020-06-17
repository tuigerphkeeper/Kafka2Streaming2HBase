package com.own.hydropower.data2hbase

import java.text.SimpleDateFormat
import java.util.Date

import com.own.hydropower.Init.ReadParameter
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * DStream数据导入hbase
 */
class Data2db() {
  @transient private val log = LogManager.getLogger(classOf[Data2db])

  private var kafkaParams = Map("metadata.broker.list" -> ReadParameter.BROKERS, "group.id" -> ReadParameter.GROUPS(0))
  // 将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
  private val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.key(), mam.message())

  def get_DStream(ssc: StreamingContext): InputDStream[(String, String)] = {
    var kafkaStream: InputDStream[(String, String)] = null
    val from_offset: Map[TopicAndPartition, Long] = new Set_offset().set_start_offset()
    if (from_offset.nonEmpty) {
      log.info("从上次offset开始消费...")
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, from_offset, messageHandler)
    } else {
      // log.info("从最小offset开始消费")
      kafkaParams += ("auto.offset.reset" -> "smallest")
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(ReadParameter.TOPICS(0)))
    }
    kafkaStream
  }

  def data2db(ssc: StreamingContext): Unit = {
    log.info("数据导入HBase开始...")
    val kafkaStream = get_DStream(ssc: StreamingContext)
    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", ReadParameter.ZOOKEEPER_QUORUM)
        conf.set("hbase.zookeeper.property.clientPort", ReadParameter.HBASE_PORT)
        conf.set("zookeeper.znode.parent", ReadParameter.HBASE_PATH)
        val myTable = new HTable(conf, TableName.valueOf(ReadParameter.TABLE_NAMES(0)))
        myTable.setAutoFlush(false, false) //关闭自动提交
        myTable.setWriteBufferSize(3 * 1024 * 1024) //数据缓存大小
        partitionOfRecords.foreach(pair => {
          val arr = pair._2.replace("[{", "").replace("}]", "").trim.split(",")
          for (i <- 1 until arr.size) {
            val date: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(arr(0).substring(arr(0).indexOf("=") + 1).toLong)) // 时间戳
            val put = new Put(Bytes.toBytes(pair._1))
            put.setWriteToWAL(false)
            put.addColumn(ReadParameter.FAMILIES(0).getBytes(), ReadParameter.COLUMNS(0).getBytes(), arr(i).substring(0, arr(i).indexOf(";")).substring(0, arr(i).indexOf("=")).trim.getBytes()) // 节点名称
            put.addColumn(ReadParameter.FAMILIES(0).getBytes(), ReadParameter.COLUMNS(1).getBytes(), date.substring(0, date.indexOf(" ")).getBytes()) // 日期
            put.addColumn(ReadParameter.FAMILIES(0).getBytes(), ReadParameter.COLUMNS(2).getBytes(), date.substring(date.indexOf(" ") + 1).getBytes()) // 时间
            put.addColumn(ReadParameter.FAMILIES(0).getBytes(), ReadParameter.COLUMNS(3).getBytes(), arr(i).substring(arr(i).indexOf("=") + 1, arr(i).indexOf(";")).trim.getBytes()) //节点数值
            put.addColumn(ReadParameter.FAMILIES(0).getBytes(), ReadParameter.COLUMNS(4).getBytes(), arr(i).substring(arr(i).indexOf(";") + 1).trim.getBytes()) // 有效性
            myTable.put(put)
          }
        })
        myTable.flushCommits()
      })
    })
    log.info("本批次数据导入完毕")
    new Manage_offset().save_offset(kafkaStream)
  }
}
