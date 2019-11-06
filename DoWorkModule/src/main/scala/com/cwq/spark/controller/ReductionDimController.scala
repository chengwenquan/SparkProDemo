package com.cwq.spark.controller

import com.cwq.spark.service.ReductionDimServer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 对章节降维
  */
object ReductionDimController {
  def main(args: Array[String]): Unit = {
    //创建spark
    val conf: SparkConf = new SparkConf().setAppName("reductionDim").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    var time = "20190722"
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    ReductionDimServer.DwsQzChapter(spark,time)
    ReductionDimServer.DwsQzMajor(spark,time)
    ReductionDimServer.DwsQzCourse(spark,time)
    ReductionDimServer.DwsQzPaper(spark,time)
    ReductionDimServer.DwsQzQuestion(spark,time)
    ReductionDimServer.DwsUserPaperDetail(spark,time)
    ReductionDimServer.DwsSelectUserPaperDetail(spark,time)
    spark.close()
  }
}
