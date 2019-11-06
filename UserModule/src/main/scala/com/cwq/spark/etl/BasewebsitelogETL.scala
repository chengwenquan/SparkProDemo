package com.cwq.spark.etl

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasewebsitelogETL {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("ETL")
      .master("local[2]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val rdd_str: RDD[String] = sc.textFile("/user/sourceFile/ods/baswewebsite.log")

    //将数据封装成对象
    val memberOBJ= rdd_str.map(JSON.parseObject(_, classOf[Basewebsitelog]))

    import spark.implicits._
    val df1: DataFrame = memberOBJ.toDF()

    df1.createOrReplaceTempView("basewebsite")

    spark.sql("use dwd")//选中库
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(
      """
        |insert into dwd.dwd_base_website
        |select
        |siteid,sitename,siteurl,delete,createtime,creator,dn
        |from basewebsite
      """.stripMargin)
    spark.close()
  }
}



case class Basewebsitelog(
            siteid :Int,
            sitename :String,
            siteurl  :String,
            delete :Int,
            createtime  :String,
            creator  :String,
            dn :String)
