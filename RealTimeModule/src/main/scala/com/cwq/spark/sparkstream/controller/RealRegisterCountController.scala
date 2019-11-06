package com.cwq.spark.sparkstream.controller

import java.lang
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时统计注册人员信息
  */
object RealRegisterCountController {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("HighKafka")
      .set( "spark.streaming.kafka.maxRatePerPartition","100")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./reginster")

    // kafka 参数
    //kafka参数声明
    val brokers = "hadoop200:9092,hadoop201:9092,hadoop202:9092"
    val topic = "test"
    val group = "bigdata7"

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:lang.Boolean),//是否自动提交偏移量，如果是true:先提交在处理，如果是false:不会提交偏移量
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val input: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams))

    input.filter(p => {
      val sp: Array[String] = p.value().split("\t")
      sp.length == 3
    }).map(p => {
      val str = p.value().split("\t")
      var pt = str(1) match {
        case "1" => "PC"
        case "2" => "APP"
        case "3" => "Other"
      }
      (pt,1)
    }).updateStateByKey[Int]((seq:Seq[Int], opt:Option[Int])=>Some(seq.sum + opt.getOrElse(0)))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

object RealRegisterCountController01{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
      .set( "spark.streaming.kafka.maxRatePerPartition","100")
    val ssc = new StreamingContext(conf, Seconds(6))
    ssc.checkpoint("./reginster")

    // kafka 参数
    //kafka参数声明
    val brokers = "hadoop200:9092,hadoop201:9092,hadoop202:9092"
    val topic = "test"
    val group = "bigdata7"

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:lang.Boolean),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val input: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams))

    input
      .filter(p => {
      val sp: Array[String] = p.value().split("\t")
      sp.length == 3
    })
      .map(p => {
      val str = p.value().split("\t")
      var pt = str(1) match {
        case "1" => "PC"
        case "2" => "APP"
        case "3" => "Other"
      }
      (pt,1)
    })
      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,(a:Int,b:Int)=> a-b,Seconds(60),Seconds(6))
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}