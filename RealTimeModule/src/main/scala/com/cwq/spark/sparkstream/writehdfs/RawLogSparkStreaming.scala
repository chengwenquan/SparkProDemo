package com.cwq.spark.sparkstream.writehdfs

import java.sql.{PreparedStatement, ResultSet}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import com.cwq.spark.util.DataSourceUtil
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 通用原始日志数据落盘到hdfs
  * 每日产生一个文件
  */
object RawLogSparkStreaming {
  private var fs: FileSystem = null
  private var fSOutputStream: FSDataOutputStream = null
  private var writePath: Path = null
  private val hdfsBasicPath = "hdfs://nameservice1/user/cwq/rawlogdata/"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "cwq")//本地跑的时候需要设置用户
    val sparkConf = new SparkConf().setAppName("RawLog_SparkStreaming")
      .set("spark.streaming.kafka.maxRatePerPartition", "20")//kafka的消费速度每个分区每秒20条数据
      .set("spark.streaming.backpressure.enabled", "true")//打开背压机制
      .set("spark.streaming.stopGracefullyOnShutdown", "true")//设置优雅关闭
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val sparkContext = ssc.sparkContext
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

//-----------------kafka配置--------------------------------------------------
    val topic = "page_topic"
    val groupid = "raw_groupid"
    val kafka_broker_list = "hadoop200:9092,hadoop201:9092,hadoop202:9092"
    val topicTable = "offset_manager"//偏移量表

    val broker_list = kafka_broker_list
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list, //用于初始化链接到集群
      "key.deserializer" -> classOf[StringDeserializer],//key序列化
      "value.deserializer" -> classOf[StringDeserializer],//value序列化
      "group.id" -> groupid,//设置消费者组
      "auto.offset.reset" -> "earliest",//读取偏移量的位置
      "enable.auto.commit" -> (false: java.lang.Boolean)//是否自动提交偏移量
    )

//-----------------读取偏移量--------------------------------------------------
    val offset_client = DataSourceUtil.getConnection()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val offset_ps: PreparedStatement = offset_client.prepareStatement(s"select *from ${topicTable} where groupid=?")
    offset_ps.setString(1,topic)
    val offset_rs: ResultSet = offset_ps.executeQuery()
    while(offset_rs.next()){
      val model = new TopicPartition(offset_rs.getString(2), offset_rs.getInt(3))
      val offset = offset_rs.getLong(4)
      offsetMap.put(model, offset)
    }
    offset_ps.close()
    offset_rs.close()
    offset_client.close()

//-----------------根据偏移量从kafka中读取数据--------------------------------------------------
    val dataDStream = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    } else {
      KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsetMap))
    }
//-----------------处理数据--------------------------------------------------
    //拼接hdfs上的保存路径
    def getTotalPath(lastTime: Long): String = {
      val dft = DateTimeFormatter.ofPattern("yyyyMMdd")
      val formatDate = dft.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(lastTime), ZoneId.systemDefault()))
      val totalPath = hdfsBasicPath + "/" + topic + "/" + formatDate // 路径/topic/日期
      totalPath
    }

    //将RDD数据保存到HDFS
    val dataValueStream = dataDStream.map(item => (item.key(), item.value()))
    dataValueStream.foreachRDD(rdd => {
      val lastTime = System.currentTimeMillis()//当前时间戳
      val writePath = getTotalPath(lastTime)//获取保存路径
      val job = new JobConf()
      job.set("mapred.output.compress", "true")//开启压缩
      job.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")//使用snappy压缩
      //将数据保存到hdfs
      rdd.saveAsHadoopFile(writePath,classOf[Text], classOf[Text], classOf[RDDMultipleAppendTextOutputFormat], job)
    })

//-----------------更新数据库中的偏移量--------------------------------------------------
    dataDStream.foreachRDD(rdd => {
      val client = DataSourceUtil.getConnection()
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (or <- offsetRanges) {
        var ps: PreparedStatement = client.prepareStatement(s"replace into `${topicTable}` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)")
        ps.setString(1,groupid)
        ps.setString(2,or.topic)
        ps.setString(3,or.partition.toString)
        ps.setLong(4,or.untilOffset)
        ps.executeUpdate()
        ps.close()
      }
      client.close()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
