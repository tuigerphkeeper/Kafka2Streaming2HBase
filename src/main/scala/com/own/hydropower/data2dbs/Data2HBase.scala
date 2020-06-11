package com.own.hydropower.data2dbs

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream

/**
 * 连接数据库，并把数据写入数据库。
 */
class Data2HBase(broadcast: Broadcast[Properties]) extends Serializable {

  @transient lazy private val log = LogManager.getLogger(classOf[Data2HBase])
  private val pp = broadcast.value

  /**
   * MapReduce程序将Kafka数据写入到HBase
   */
  def writeDatabase(kafkaStream: InputDStream[(String, String)]): Unit = {
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partitionOfRecords => {
          val conf: Configuration = HBaseConfiguration.create()
          log.info("hbase节点" + pp.getProperty("quorum"))
          conf.set("hbase.zookeeper.quorum", pp.getProperty("quorum"))
          conf.set("hbase.zookeeper.property.clientPort", pp.getProperty("port"))
          conf.set("zookeeper.znode.parent", pp.getProperty("hbasePath"))
          val myTable = new HTable(conf, TableName.valueOf(pp.getProperty("tableName")))
          myTable.setAutoFlush(false, false) //关闭自动提交
          myTable.setWriteBufferSize(3 * 1024 * 1024) //数据缓存大小
          partitionOfRecords.foreach(pair => {
            val arr = pair._2.replace("[{", "").replace("}]", "").trim.split(",")
            for (i <- 1 until arr.size) {
              val date: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(arr(0).substring(arr(0).indexOf("=") + 1).toLong)) //时间戳
              val put = new Put(Bytes.toBytes(pair._1))
              put.setWriteToWAL(false)
              put.addColumn(pp.getProperty("family").getBytes(), pp.getProperty("column1").getBytes(), arr(i).substring(0, arr(i).indexOf(";")).substring(0, arr(i).indexOf("=")).trim.getBytes()) //节点名称
              put.addColumn(pp.getProperty("family").getBytes(), pp.getProperty("column2").getBytes(), date.substring(0, date.indexOf(" ")).getBytes()) //日期
              put.addColumn(pp.getProperty("family").getBytes(), pp.getProperty("column3").getBytes(), date.substring(date.indexOf(" ") + 1).getBytes()) //时间
              put.addColumn(pp.getProperty("family").getBytes(), pp.getProperty("column4").getBytes(), arr(i).substring(arr(i).indexOf("=") + 1, arr(i).indexOf(";")).trim.getBytes()) //节点数值
              put.addColumn(pp.getProperty("family").getBytes(), pp.getProperty("column5").getBytes(), arr(i).substring(arr(i).indexOf(";") + 1).trim.getBytes()) //有效性
              myTable.put(put)
            }
          })
          myTable.flushCommits()
        })
      } else {
        System.exit(-1)
      }
    })


    /**
     * Kafka获取数据导入Mysql
     */
    //  def writeDataToMysql(kafkaStream: InputDStream[(String, String)]): Unit = {
    //    kafkaStream.foreachRDD(rdd => {
    //      def func(records: Iterator[(String, Int)]): Unit = {
    //        //注意，conn和stmt定义为var不能是val
    //        var conn: Connection = null
    //        var stmt: PreparedStatement = null
    //        try {
    //          //连接数据库
    //          conn = DriverManager.getConnection("jdbc:mysql://cdh1:3306/test", "user", "123456")
    //          //
    //          records.foreach(p => {
    //            //wordcount为表名，word和count为要插入数据的属性
    //            //插入数据
    //            val sql = "insert into temp(temp,num) values(?,?)"
    //            stmt = conn.prepareStatement(sql)
    //            stmt.setString(1,p._1.trim)
    //            stmt.setInt(2,p._2.toInt)
    //           // stmt.setString(3, tempArr(0).substring(tempArr(i).indexOf("=") + 1).trim)
    //            stmt.executeUpdate()
    //          })
    //        } catch {
    //          case e: Exception => e.printStackTrace()
    //        } finally {
    //          if (stmt != null)
    //            stmt.close()
    //          if (conn != null)
    //            conn.close()
    //        }
    //      }
    //    })
    //  }
  }
}
