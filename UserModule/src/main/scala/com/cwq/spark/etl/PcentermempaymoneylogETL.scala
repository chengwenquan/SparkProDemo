package com.cwq.spark.etl

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object PcentermempaymoneylogETL {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("ETL")
      .master("local[2]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val rdd_str: RDD[String] = sc.textFile("/user/sourceFile/ods/pcentermempaymoney.log")

    //将数据封装成对象
    val memberOBJ= rdd_str.map(JSON.parseObject(_, classOf[Pcentermempaymoneylog]))

    import spark.implicits._
    val df1: DataFrame = memberOBJ.toDF()

    df1.createOrReplaceTempView("pcentermempaymoneylog")

    spark.sql("use dwd")//选中库
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(
      """
        |insert into dwd.dwd_pcentermempaymoney
        |select
        |uid,paymoney,siteid,vip_id,dt,dn
        |from pcentermempaymoneylog
      """.stripMargin)
    spark.close()
  }
}

case class Pcentermempaymoneylog(
                                  uid :Int,
                                  paymoney :String,
                                  siteid :Int,
                                  vip_id :Int,
                                  dt :String,
                                  dn :String
                                )