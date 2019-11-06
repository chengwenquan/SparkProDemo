package com.cwq.spark.sparkstream.controller


import java.lang
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.NumberFormat

import com.alibaba.fastjson.JSON
import com.cwq.spark.sparkstream.bean.Page
import com.cwq.spark.util.DataSourceUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFiles}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * 页面转换率实时统计
  *   kafka中的数据格式：{"app_id":"1","device_id":"102","distinct_id":"5fa401c8-dd45-4425-b8c6-700f9f74c532","event_name":"-","ip":"121.76.152.135","last_event_name":"-","last_page_id":"0","next_event_name":"-","next_page_id":"2","page_id":"1","server_time":"-","uid":"245494"}
  * 1.查询数据库中的偏移量，根据偏移量消费数据
  * 2.计算每个页面的跳转次数，然后从mysql数据库查出历史的点击次数，本次计算的点击次数和历史的点击次数相加，把相加的结果更新到数据库，完成页面跳转统计
  * 3.统计点击数在前三的省份，使用ip2region解析ip，解析后的数据为：中国|0|上海|上海市|有线通。
  * 4.更新数据库中的偏移量
  */
object PageStreaming {
  private val groupid = "page_groupid"
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100") //控制从kafka读取的速度
      .set("spark.streaming.backpressure.enabled", "true") //开启背压机制
      .set("spark.streaming.stopGracefullyOnShutdown", "true")//优雅关闭
      .setMaster("local[*]")//本地运行需要打开
    val ssc = new StreamingContext(conf, Seconds(3))//批次大小
//    val sparkContext = ssc.sparkContext
//    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
//    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    val topics = Array("page_topic")//主题
    //kafka的相关配置
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop200:9092,hadoop201:9092,hadoop202:9092", //kafka集群
      "key.deserializer" -> classOf[StringDeserializer],//key序列化
      "value.deserializer" -> classOf[StringDeserializer],//value序列化
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",//消费位置
      "enable.auto.commit" -> (false: lang.Boolean)//关闭自动提交
    )

//-------------------------------从MySQL中查询偏移量--------------------------------------------------------------------------------
    //查询mysql中是否存在偏移量
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    val ps: PreparedStatement = client.prepareStatement("select * from `offset_manager` where groupid=?")
    ps.setString(1,groupid)
    val rs: ResultSet = ps.executeQuery()
    while(rs.next()){
      val model = new TopicPartition(rs.getString(2),rs.getInt(3))
      val offset: Long = rs.getLong(4)
      offsetMap.put(model,offset)
    }
    rs.close()
    ps.close()
    client.close()

//-------------------------------根据偏移量从kafka读取数据--------------------------------------------------------------------------------

    //设置kafka消费数据的参数 判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

//-------------------------------业务处理--------------------------------------------------------------------------------
    //解析json数据
    val dsStream = stream.map(item => item.value()).mapPartitions(partition => {
      partition.map(item => {
        JSON.parseObject(item,classOf[Page])
        //(uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    })
    //缓存
    dsStream.cache()
    //封装（上一页—当前页—下一页，1）
    val pageValueDStream = dsStream.map(item => (item.last_page_id + "_" + item.page_id + "_" + item.next_page_id, 1))
    //统计页面跳转的个数（相同规则的跳转）
    val resultDStream = pageValueDStream.reduceByKey(_ + _)
    //对Stream中的RDD进行遍历
    resultDStream.foreachRDD(rdd => {
      //对每个分区进行遍历
      rdd.foreachPartition(partition => {
        //获取数据库连接
        val client = DataSourceUtil.getConnection
        //处理数据
        partition.foreach(item => {
          calcPageJumpCount(item, client) //计算页面跳转个数
        })
        //关闭连接
        client.close()
      })
    })

    //-------------解析ip,统计每个省份的点击个数-------------------------
    //ssc.sparkContext.addFile("hdfs://nameservice1/user/atguigu/sparkstreaming/ip2region.db")//集群中运行加上
    ssc.sparkContext.addFile("D:\\My_pro\\IDEA_Project\\SparkProDemo\\RealTimeModule\\src\\main\\resources\\ip2region.db") //广播文件
    val ipDStream = dsStream.mapPartitions(patitions => {
      //获取并广播文件
      val dbFile = SparkFiles.get("ip2region.db")
      val ipsearch = new DbSearcher(new DbConfig(), dbFile)

      patitions.map { item =>
        val ip = item.ip
        val province = ipsearch.memorySearch(ip).getRegion().split("\\|")(2) //获取ip详情   中国|0|上海|上海市|有线通
        (province, 1l) //根据省份 统计点击个数
      }
    }).reduceByKey(_ + _)


    ipDStream.foreachRDD(rdd => {
      //查询mysql历史数据 累加
      val ipClient = DataSourceUtil.getConnection
      val history_data = new ArrayBuffer[(String, Long)]()
      val ipps: PreparedStatement = ipClient.prepareStatement("select province,num from tmp_city_num_detail")

      val iprs: ResultSet = ipps.executeQuery()
      while (iprs.next()) {
        val tuple = (iprs.getString(1), iprs.getLong(2))
        history_data += tuple
      }
      //关闭连接
      iprs.close()
      ipps.close()
      ipClient.close()

      val history_rdd: RDD[(String, Long)] = ssc.sparkContext.makeRDD(history_data)
      //将现有的rdd和历史的rdd进行全外连接
      val joinrdd: RDD[(String, (Option[Long], Option[Long]))] = history_rdd.fullOuterJoin(rdd)
      val resultRdd: RDD[(String, Long)] = joinrdd.map(item => {
        val province = item._1 //省份
        val nums = item._2._1.getOrElse(0l) + item._2._2.getOrElse(0l) //将点击数和历史点击数相加
        (province, nums)
      })
      //将累加后的结果写入数据库中
      resultRdd.foreachPartition(partition => {
        //获取数据库连接
        val dripClient = DataSourceUtil.getConnection
        //循环遍历写入数库
        partition.foreach(item=>{
          val province = item._1
          val num = item._2
          //修改mysql数据 并重组返回最新结果数据
          val dripps: PreparedStatement = dripClient.prepareStatement("insert into tmp_city_num_detail(province,num)values(?,?) on duplicate key update num=?")
          dripps.setString(1,province)
          dripps.setLong(2,num)
          dripps.setLong(3,num)
          dripps.executeUpdate()
          dripps.close()
        })
        //关闭数据库
        dripClient.close()
      })

      //更新点击数是前三的省份数据
      val top3Rdd = resultRdd.sortBy[Long](_._2, false).take(3)
      val top3Client = DataSourceUtil.getConnection
      //清空表
      val top3ps: PreparedStatement = top3Client.prepareStatement("truncate table top_city_num")
      top3ps.executeUpdate()
      //重新插入数据
      top3Rdd.foreach(item =>{
        val topps: PreparedStatement = top3Client.prepareStatement("insert into top_city_num (province,num) values(?,?)")
        topps.setString(1,item._1)
        topps.setLong(2,item._2)
        topps.executeUpdate()
        topps.close()
      })
      top3Client.close()//关闭连接
    })

    //计算转换率
    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val saveclient = DataSourceUtil.getConnection
        calcJumRate(saveclient) //计算转换率并保存

        //保存偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          val saveoffset_ps = saveclient.prepareStatement("replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)")
          saveoffset_ps.setString(1,groupid)
          saveoffset_ps.setString(2,or.topic)
          saveoffset_ps.setString(3,or.partition.toString)
          saveoffset_ps.setLong(4,or.untilOffset)
          saveoffset_ps.executeUpdate()
          saveoffset_ps.close()
        }
      saveclient.close()
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 计算页面跳转个数
    *
    * @param item
    * @param client
    */
  def calcPageJumpCount(item: (String, Int), client: Connection): Unit = {
    val keys = item._1.split("_") //对key进行切分
    var num: Long = item._2 //页面跳转的个数
    val page_id = keys(1).toInt //获取当前page_id
    val last_page_id = keys(0).toInt //获取上一page_id
    val next_page_id = keys(2).toInt //获取下页面page_id
    //查询当前page_id的历史num个数
    val ps: PreparedStatement = client.prepareStatement("select num from page_jump_rate where page_id=?")
    ps.setInt(1,page_id)
    val rs: ResultSet = ps.executeQuery()
    while (rs.next()) {
      num += rs.getLong(1)
    }
    rs.close()
    ps.close()

    //对num 进行修改 并且判断当前page_id是否为首页
    if (page_id == 1) {
      //更新库中的页面跳转个数
      val ps1: PreparedStatement = client.prepareStatement("insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate) values (?,?,?,?,?) on duplicate key update num=?")
      ps1.setInt(1,last_page_id)
      ps1.setInt(2,page_id)
      ps1.setInt(3,next_page_id)
      ps1.setLong(4,num)
      ps1.setString(5,"100%")
      ps1.setLong(6,num)
      ps1.executeUpdate()
      ps1.close()
    } else {
      val ps1: PreparedStatement = client.prepareStatement("insert into page_jump_rate(last_page_id,page_id,next_page_id,num) values (?,?,?,?) on duplicate key update num=?")
      ps1.setInt(1,last_page_id)
      ps1.setInt(2,page_id)
      ps1.setInt(3,next_page_id)
      ps1.setLong(4,num)
      ps1.setLong(5,num)
      ps1.executeUpdate()
      ps1.close()
    }
  }

  /**
    * 计算转换率
    */
  def calcJumRate(client: Connection): Unit = {
    var page1_num = 0l
    var page2_num = 0l
    var page3_num = 0l
    //计算首页的点击数 只计算页面1，2，3的点击
    val ps: PreparedStatement = client.prepareStatement("select page_id,num from page_jump_rate where page_id in (1,2,3)")
    val rs: ResultSet = ps.executeQuery()
    while(rs.next()){
      val pagenum = rs.getInt(1)
      pagenum match {
        case 1 => page1_num = rs.getLong(2)
        case 2 => page2_num = rs.getLong(2)
        case 3 => page3_num = rs.getLong(2)
        case _ => 1
      }
    }
    ps.close()
    rs.close()
    //计算跳转率
    val nf = NumberFormat.getPercentInstance
    val page1ToPage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2ToPage3Rate = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)
    //保存跳转率
    val page2Rate: PreparedStatement = client.prepareStatement("update page_jump_rate set jump_rate=? where page_id=?")
    page2Rate.setString(1,page1ToPage2Rate)
    page2Rate.setInt(2,2)
    page2Rate.executeUpdate()
    page2Rate.close()

    val page3Rate: PreparedStatement = client.prepareStatement("update page_jump_rate set jump_rate=? where page_id=?")
    page3Rate.setString(1,page2ToPage3Rate)
    page3Rate.setInt(2,3)
    page3Rate.executeUpdate()
    page3Rate.close()
  }

}