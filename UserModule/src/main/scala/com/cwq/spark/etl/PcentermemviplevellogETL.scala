package com.cwq.spark.etl

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object PcentermemviplevellogETL {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("viplevellogETL")
      .master("local[2]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val rdd_str: RDD[String] = sc.textFile("/user/sourceFile/ods/pcenterMemViplevel.log")

    //将数据封装成对象
    val memberOBJ= rdd_str.map(JSON.parseObject(_, classOf[Viplevel]))

    import spark.implicits._
    val df1: DataFrame = memberOBJ.toDF()

    df1.createOrReplaceTempView("viplevellog")

    spark.sql("use dwd")//选中库
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(
      """
        |insert into dwd.dwd_vip_level
        |select
        |vip_id ,
        |vip_level ,
        |start_time ,
        |end_time ,
        |last_modify_time ,
        |max_free ,
        |min_free ,
        |next_level ,
        |operator ,
        |dn
        |from viplevellog
      """.stripMargin)
    spark.close()

  }

}


case class Viplevel(
                     vip_id :Int,
                      vip_level :String,
                      start_time :String,
                      end_time :String,
                      last_modify_time :String,
                      max_free :String,
                      min_free :String,
                      next_level :String,
                      operator :String,
                      dn :String)

