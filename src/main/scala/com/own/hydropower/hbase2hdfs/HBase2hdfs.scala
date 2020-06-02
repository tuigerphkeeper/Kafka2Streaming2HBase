//package com.own.hydropower.hbase2hdfs
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import com.own.hydropower.common_params.Comm_Params
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.{Result, Scan}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.io.Text
//import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
//import org.apache.hadoop.mapreduce.{Job, Mapper}
//
//class HBase2hdfs {
//
//  Comm_Params.initConfig()
//
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "/usr/hdp/2.6.5.0-292/hadoop")
//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", Comm_Params.hbaseQuorum)
//    conf.set("hbase.zookeeper.property.clientPort", Comm_Params.hbasePort)
//    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
//    conf.set("zookeeper.znode.parent", Comm_Params.hbasePath)
//    conf.set("dfs.socket.timeout", "180000") //将该值改大，防止hbase超时退出
//    val job = Job.getInstance(conf, this.getClass.getSimpleName)
//    job.setJarByClass(this.getClass)
//    job.setMapperClass(classOf[HBaseToHdfsMapper])
//    job.setMapOutputKeyClass(classOf[Text])
//    job.setMapOutputValueClass(classOf[Text])
//    job.setNumReduceTasks(0)
//    TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(Comm_Params.tableName), new Scan(), classOf[HBaseToHdfsMapper], classOf[Text], classOf[Text], job, false)
//    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
//    FileOutputFormat.setOutputPath(job, new Path(Comm_Params.hdfsPath + new SimpleDateFormat("yyyy-MM-dd").format(new Date())))
//    job.waitForCompletion(true)
//    Runtime.getRuntime.exec("hadoop fs -rm -f " + Comm_Params.hdfsPath + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "/_SUCCESS")
//  }
//
//  class HBaseToHdfsMapper extends TableMapper[Text, Text] {
//    override protected def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, Text, Text]#Context): Unit = {
//      //      val line = new String(value.getColumnLatestCell(ParameterFactory.family.getBytes, "line".getBytes).getValue)
//      //      val arr = line.split(",")
//      //      for (i <- 1 until arr.size) {
//      //        val temp = arr(0).substring(arr(0).indexOf("=") + 1) + //时间time
//      //          Math.abs(arr(i).substring(0, arr(i).indexOf("=")).trim.hashCode) + "," + //节点hash值
//      //          arr(i).substring(0, arr(i).indexOf("=")).trim + "," + //节点名称
//      //          arr(i).substring(arr(i).indexOf("=") + 1).trim //节点数值
//      //        context.write(null, new Text(temp))
//      //      }
//
//      val SName = value.getColumnLatestCell(Comm_Params.family.getBytes, Comm_Params.column1.getBytes).getValue
//      val Date = value.getColumnLatestCell(Comm_Params.family.getBytes, Comm_Params.column2.getBytes).getValue
//      val Time = value.getColumnLatestCell(Comm_Params.family.getBytes, Comm_Params.column3.getBytes).getValue
//      val Data = value.getColumnLatestCell(Comm_Params.family.getBytes, Comm_Params.column4.getBytes).getValue
//      val Flag = value.getColumnLatestCell(Comm_Params.family.getBytes, Comm_Params.column5.getBytes).getValue
//
//      val temp = (if (SName == null || (SName.length == 0)) "NULL"
//      else new String(SName)) + "," + (if (Date == null || (Date.length == 0)) "NULL"
//      else new String(Date)) + "," + (if (Time == null || (Time.length == 0)) "NULL"
//      else new String(Time)) + "," + (if (Data == null || (Data.length == 0)) "NULL"
//      else new String(Data)) + "," + (if (Flag == null || (Flag.length == 0)) "NULL"
//      else new String(Flag))
//
//      context.write(null, new Text(temp))
//    }
//  }
//
//}
