package com.cwq.spark.etl

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object BaseadlogEtl {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("baseadlog").master("local[*]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val value: RDD[String] = sc.textFile("/user/sourceFile/ods/baseadlog.log")

    val rdd_obj = value.map(JSON.parseObject(_,classOf[Baseadlog]))

    import spark.implicits._
    val dataFrame: DataFrame = rdd_obj.toDF()
    dataFrame.createOrReplaceTempView("baseadlog")

    spark.sql("use dwd")//选中库
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(
      """
        |insert into dwd.dwd_base_ad
        |select adid,adname,dn from baseadlog
      """.stripMargin)


    spark.sql("select * from user").show

    spark.close()
  }

}

case class Baseadlog(
                    var adid:Int,//基础广告表广告id
                    var adname:String,//广告详情名称
                    var dn:String //网站分区
                    )


