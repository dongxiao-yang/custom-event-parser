//package com.conviva.platform
//
//import com.conviva.scala_messages.livepass.{SessOutKeyPb, SessOutValPb}
//import org.apache.spark.sql.SparkSession
//
//object CustomMetricParser {
//
//  def main(args: Array[String]): Unit = {
//    val spark: SparkSession = SparkSession.builder.appName("SSM2-Reader")
//
//      .master("local[2]").getOrCreate()
//    //    val data = spark.read.textFile("testaaa.txt").rdd
//    //    val number = data.count();
//    //    data.foreach(println(_))
//    //    println("number "+number);
//
//
//    //
//    //    val data =  spark.sparkContext.sequenceFile("part_00000.seq",classOf[SessOutKeyPb], classOf[SessOutValPb])
//    //    println(data.count())
//    val format = PacketBrainFormat
//    val data = HdfsSessionSummaryReader.extract("hdfs://rccp103-9c.iad4.prod.conviva.com:8020/tlb1min/ssms/ssm2-sesssummary/2022/03/03/04/04/part_00000.seq", format)
//    //
//    val aaa = data.foreach(s => {
//      val (k, v) = s
//
//      val keytype = k._type
//      val valuetype = v._type
//      println("key_type:" + keytype)
//      println("value_type:" + valuetype)
//    })
//    spark.stop()
//  }
//}
