package com.own.hydropower

import com.own.hydropower.Init.{Initialization, ReadParameter}
import com.own.hydropower.data2hbase.Data2db
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\WorkSpace\\hadoop-3.1.1")
    val sc = Initialization.sc
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    ssc.sparkContext.broadcast(ReadParameter)
    new Data2db().data2db(ssc)
    ssc.start()
    ssc.awaitTermination()
  }
}
