package com.cwq.spark.sparkstream.controller

import java.lang
import java.sql.{PreparedStatement, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cwq.spark.util.DataSourceUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object KafkaOffSetSaveController {
  def main(args: Array[String]): Unit = {
//-----------------------------配置相关-------------------------------------------------------
    val conf: SparkConf = new SparkConf().setAppName("kafkaOffset").setMaster("local[10]")
      .set("spark.streaming.kafka.maxRatePerPartition","100")//控制从卡夫卡中读取数据的速度每秒100条
      .set("spark.streaming.backpressure.enabled","true")//开启背压机制
    val ssc = new StreamingContext(conf,Seconds(3))//3秒一个批次，并且3秒执行一次

    //设置kafka相关的参数
    val topic = "qz_log"
    val groupId = "suibian1"
    val map: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop200:9092,hadoop201:9092,hadoop202:9092",//kafka集群
      "key.deserializer" -> classOf[StringDeserializer],//key序列化
      "value.deserializer" -> classOf[StringDeserializer],//value序列化
      "group.id" -> groupId,//指定消费者组
      "auto.offset.reset" -> "earliest",//kafka的读取位置有三种：earliest\latest\none
      "enable.auto.commit" -> (false: lang.Boolean) //是否开启自动提交，如果为true，即为开启自动提交，在处理数据之前就会提交偏移量。如果为false,不会提交偏移量
    )

    //存放偏移量
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
//-----------------------------从mysql中读取kafka的偏移量-------------------------------------------------------
    //获取数据库连接
    val client = DataSourceUtil.getConnection
    //预编译SQL
    val ps: PreparedStatement = client.prepareStatement("select * from offset_manager where groupid=?")
    ps.setString(1,groupId)
//  mysql中存放偏移量的表中字段：groupid   topic    partition    untiloffset
    //执行SQL，将结果集写入offsetMap中
    val resultSet: ResultSet = ps.executeQuery()
    while (resultSet.next()){
      val model = new TopicPartition(resultSet.getString(2), resultSet.getInt(3))
      val offset = resultSet.getLong(4)
      offsetMap.put(model,offset) //保存偏移量
    }
    ps.close()//关闭ps
    resultSet.close()//关闭rs
    client.close()//关闭连接
//-----------------------------获取数据并处理业务------------------------------------------------------
    //根据偏移量消费数据
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {//如果为空就说明第一次读
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Seq(topic), map))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Seq(topic), map, offsetMap))
    }

    //过滤掉脏数据
    val filtered: DStream[ConsumerRecord[String, String]] = stream.filter(e => {
      val value: String = e.value() //从卡夫卡中接收的数据不能直接输出，需要获取value后再输出
      value.split("\t").length == 6
    })

    //将数据封装层元组
    val tuple: DStream[(String, String, String, String, String, String)] = filtered.mapPartitions(partition => {
      partition.map(item => {
        val arr = item.value().split("\t")
        val uid = arr(0) //用户id
        val courseid = arr(1) //课程id
        val pointid = arr(2) //知识点id
        val questionid = arr(3) //题目id
        val istrue = arr(4) //是否正确
        val createtime = arr(5) //创建时间
        (uid, courseid, pointid, questionid, istrue, createtime)
      })
    })

    //遍历每个RDD
    tuple.foreachRDD(rdd=>{
      //将RDD中的数据按用户，课程id，知识点进行分组（目的是将同一个用户的数据分配到一个分区，这样可以防止同一个用户的数据被分到不同的分区上，多次写入造成线程不安全）
      val groupByUser: RDD[(String, Iterable[(String, String, String, String, String, String)])] = rdd.groupBy(e => e._1 + "_" + e._2 + "_" + e._3)
      //对每个分区中的数据进行处理
      groupByUser.foreachPartition(partition =>{
        //对分区中的数据进行操作
        partition.foreach(e => {
          val keys = e._1.split("_")
          val userid = keys(0).toInt  //用户id
          val courseid = keys(1).toInt  //课程id
          val pointid = keys(2).toInt //知识点id
          val values: Iterable[(String, String, String, String, String, String)] = e._2
          val qs = values.map(e=>e._4).toArray.distinct //收集所有的题目id并去重

          //查询历史题目（之前做过的题目）
          val client = DataSourceUtil.getConnection
          val ps: PreparedStatement = client.prepareStatement("select questionids from qz_point_history where userid=? and courseid=? and pointid=?")
          ps.setInt(1,userid)
          ps.setInt(2,courseid)
          ps.setInt(3,pointid)
          val resultSet: ResultSet = ps.executeQuery()
          //获取历史做题情况
          var historyQs:Array[String] = Array();
            while (resultSet.next()){
              historyQs = resultSet.getString(1).split(",")
          }
          ps.close()//关闭ps
          resultSet.close()//关闭rs

          //将历史做的题目和现在的进行拼接
          val qsall: Array[String] = qs.union(historyQs).distinct
          val qs_sum: Int = qsall.length//总的做题个数
          val resultQuestionid_str: String = qsall.mkString(",")//将题目用","拼接,方便存入数据库

          var qz_sum: Int = values.size //获取当前批次题总数
          var qz_count: Int = qs.length //去重后的题个数

          var qz_istrue = values.filter(_._5.equals("1")).size //获取当前批次做正确的题个数
          val createtime = values.map(_._6).min //获取最早的创建时间 作为表中创建时间

          val currentDate = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())//获取当前时间

          val ps1: PreparedStatement = client.prepareStatement(
           s"""
             |insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) on duplicate key update questionids=?,updatetime=?
           """.stripMargin)
          ps1.setInt(1,userid)
          ps1.setInt(2,courseid)
          ps1.setInt(3,userid)
          ps1.setString(4,resultQuestionid_str)
          ps1.setString(5,createtime)
          ps1.setString(6,createtime)
          ps1.setString(7,resultQuestionid_str)
          ps1.setString(8,currentDate)

          ps1.executeUpdate()
          ps1.close()

          var qzSum_history = 0
          var istrue_history = 0
          val ps2: PreparedStatement = client.prepareStatement("select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?")
          ps2.setInt(1,userid)
          ps2.setInt(2,courseid)
          ps2.setInt(3,pointid)

          val rset: ResultSet = ps2.executeQuery()
          while (rset.next()) {
            qzSum_history += rset.getInt(1)
            istrue_history += rset.getInt(2)
          }
          rset.close()
          ps2.close()

          qz_sum += qzSum_history
          qz_istrue += istrue_history

          val correct_rate = qz_istrue.toDouble / qz_sum.toDouble //计算正确率
          //计算完成率
          //假设每个知识点下一共有30道题  先计算题的做题情况 再计知识点掌握度
          val qz_detail_rate = qs_sum.toDouble / 30 //算出做题情况乘以 正确率 得出完成率 假如30道题都做了那么正确率等于 知识点掌握度
          val mastery_rate = qz_detail_rate * correct_rate

          var ps3 = client.prepareStatement("insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
            " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
            Array(userid+"", courseid+"", pointid+"", qz_sum+"", qs_sum+"", qz_istrue+"", correct_rate+"", mastery_rate+"",
              createtime, currentDate, qz_sum+"", qs_sum+"", qz_istrue+"", correct_rate+"", mastery_rate+"", currentDate))
          ps3.setInt(1,userid)
          ps3.setInt(2,courseid)
          ps3.setInt(3,pointid)
          ps3.setInt(4,qz_sum)
          ps3.setInt(5,qs_sum)
          ps3.setInt(6,qz_istrue)
          ps3.setDouble(7,correct_rate)
          ps3.setDouble(8,mastery_rate)
          ps3.setString(9,createtime)
          ps3.setString(10,currentDate)
          ps3.setInt(11,qz_sum)
          ps3.setInt(12,qs_sum)
          ps3.setInt(13,qz_istrue)
          ps3.setDouble(14,correct_rate)
          ps3.setDouble(15,mastery_rate)
          ps3.setString(16,currentDate)
          ps3.executeUpdate()

          ps3.close()
          client.close()//关闭连接
        })
      })
    })
//-----------------------------更新mysql数据库中kafka的偏移量-------------------------------------------------------
    //更新mysql中的偏移量
    stream.foreachRDD(rdd=>{
      //获取数据库连接
      val client = DataSourceUtil.getConnection
      //获取rdd中各个分区中的偏移量，然后更新mysql中的偏移量
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (or <- offsetRanges) {
        val ps: PreparedStatement = client.prepareStatement("replace into offset_manager values(?,?,?,?)")
        ps.setString(1,groupId)
        ps.setString(2,or.topic)
        ps.setInt(3,or.partition)
        ps.setLong(4,or.untilOffset)
        ps.executeUpdate()
        ps.close()  //关闭ps
      }
      client.close()//关闭数据库连接
    })

//-----------------------------启动并等待-------------------------------------------------------
    ssc.start()
    ssc.awaitTermination()
  }
}
