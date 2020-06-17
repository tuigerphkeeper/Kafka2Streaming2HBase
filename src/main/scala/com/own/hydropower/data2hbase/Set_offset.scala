package com.own.hydropower.data2hbase

import com.own.hydropower.Init.{Initialization, ReadParameter}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import org.apache.log4j.LogManager

/**
 * 设置kafka开始消费的offset
 */
class Set_offset {
  @transient private val log = LogManager.getLogger(classOf[Set_offset])
  /**
   * 通过比较kafka的offset和zk上存储的offset设置本次从何开始读取数据
   *
   * @return 修正后的offset
   */
  def set_start_offset(): Map[TopicAndPartition, Long] = {
    var start_Offsets = new Manage_offset().get_current_offset()

    // 查询该路径下是否有节点（默认有字节点为我们自己保存不同 partition 时生成的）
    val children: Int = Initialization.get_zk_conn._1.countChildren(s"${Initialization.get_zk_conn._2.consumerOffsetDir}")
    log.info(s"${Initialization.get_zk_conn._2.consumerOffsetDir}")
    if (children > 0) {
      // 如果保存过 offset，这里更好的做法，还应该和kafka上最小的offset做对比，不然会报OutOfRange的错误
      for (i <- 0 until children) {
        // 获取zk上的存储的offset
        val partitionOffset: String = Initialization.get_zk_conn._1.readData[String](s"${Initialization.get_zk_conn._2.consumerOffsetDir}/$i")
        log.info(s"${Initialization.get_zk_conn._2.consumerOffsetDir}/$i" + "--" + partitionOffset)
        var partOffset = partitionOffset.toLong
        val tp: TopicAndPartition = TopicAndPartition(ReadParameter.TOPICS(0), i)
        // 获取kafka最小的offset
        val earliestOffset: OffsetRequest = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val earOffset: Seq[Long] = Initialization.get_zk_conn._3.getOffsetsBefore(earliestOffset).partitionErrorAndOffsets(tp).offsets
        // 获取kafka最大的offset
        val latestOffset: OffsetRequest = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
        val latOffset: Seq[Long] = Initialization.get_zk_conn._3.getOffsetsBefore(latestOffset).partitionErrorAndOffsets(tp).offsets
        // 通过比较从 kafka 上该 partition 的最小 offset ,当前offset和 zk 上保存的 offset，进行选择
        log.info(earOffset + "------" + latOffset + "------" + partOffset)
        if (earOffset.nonEmpty && partOffset < earOffset.head) {
          partOffset = earOffset.head
        } else if (latOffset.nonEmpty && partOffset > latOffset.head) {
          partOffset = latOffset.head
        } else {
          partOffset = 0
        }
        start_Offsets += (tp -> partOffset)
      }
      start_Offsets
    } else {
      Map[TopicAndPartition, Long]()
    }
  }
}
