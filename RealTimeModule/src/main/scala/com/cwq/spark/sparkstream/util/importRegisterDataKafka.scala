package com.cwq.spark.sparkstream.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 向kafka导入注册数据
  */
object importRegisterDataKafka {
    def main(args: Array[String]): Unit = {

      val sparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
      val ssc = new SparkContext(sparkConf)

      ssc.textFile("D:\\My_pro\\IDEA_Project\\SparkProDemo\\RealTimeModule\\src\\main\\resources\\reginster.txt")
        .foreachPartition(partition => {
          val props = new Properties()
          props.put("bootstrap.servers", "hadoop200:9092,hadoop201:9092,hadoop202:9092") //kafka集群地址
          props.put("acks", "1")//ack应答机制 0、1、-1(all)
          props.put("batch.size", "16384")//批次大小
          props.put("linger.ms", "10")//等待时间
          props.put("buffer.memory", "33554432")//缓存区大小
          props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")//key序列化
          props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")//value序列化

          val producer = new KafkaProducer[String, String](props)
          //将分区内的数据写入kafka
          partition.foreach(item => {
            //将数据封装成ProducerRecord
            val msg = new ProducerRecord[String, String]("test",item)
            //将数据交给生产者
            producer.send(msg)
          })

          producer.flush()
          producer.close()
        })
  }
}
