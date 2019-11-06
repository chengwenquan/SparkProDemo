package com.cwq.spark.controller

import com.cwq.spark.service.ETLAndImportDataServer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 将日志导入hive表中
  * 层：DWD
  */
object DwdImportDataController {
  def main(args: Array[String]): Unit = {
    //创建spark
    val conf: SparkConf = new SparkConf().setAppName("importData")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //创建sc
    val sc: SparkContext = spark.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    //导入数据
    ETLAndImportDataServer.ETLQzBusiness(spark,sc)
    ETLAndImportDataServer.ETLQzCenter(spark,sc)
    ETLAndImportDataServer.ETLQzCenterPaper(spark,sc)
    ETLAndImportDataServer.ETLQzChapter(spark,sc)
    ETLAndImportDataServer.ETLQzChapterList(spark,sc)

    ETLAndImportDataServer.ETLQzCourse(spark,sc)
    ETLAndImportDataServer.ETLQzCourseEdusubject(spark,sc)
    ETLAndImportDataServer.ETLQzMajor(spark,sc)
    ETLAndImportDataServer.ETLQzMemberPaperQuestion(spark,sc)
    ETLAndImportDataServer.ETLQzPaper(spark,sc)

    ETLAndImportDataServer.ETLQzPaperView(spark,sc)
    ETLAndImportDataServer.ETLQzPoint(spark,sc)
    ETLAndImportDataServer.ETLQzPointQuestion(spark,sc)
    ETLAndImportDataServer.ETLQzQuestion(spark,sc)
    ETLAndImportDataServer.ETLQzQuestionType(spark,sc)

    ETLAndImportDataServer.ETLQzSiteCourse(spark,sc)
    ETLAndImportDataServer.ETLQzWebsite(spark,sc)

    spark.close()
  }
}
