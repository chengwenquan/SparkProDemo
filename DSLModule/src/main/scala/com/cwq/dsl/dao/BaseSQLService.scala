package com.cwq.dsl.dao

import org.apache.spark.sql.{DataFrame, SparkSession}

object BaseSQLService {

  def SelectUser(spark: SparkSession ):DataFrame={
    val dataFrame: DataFrame = spark.read.json("D:\\My_pro\\IDEA_Project\\SparkProDemo\\DSLModule\\src\\main\\resources\\user.json")
    dataFrame
  }

  def SelectDept(spark: SparkSession ):DataFrame={
    val dataFrame: DataFrame = spark.read.json("D:\\My_pro\\IDEA_Project\\SparkProDemo\\DSLModule\\src\\main\\resources\\dept.json")
    dataFrame
  }

}
