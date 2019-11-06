package com.cwq.spark.etl

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MemberRegtypeETL {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("ETL")
      .master("local[2]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val rdd_str: RDD[String] = sc.textFile("/user/sourceFile/ods/memberRegtype.log")

    //将数据封装成对象
    val memberOBJ= rdd_str.map(JSON.parseObject(_, classOf[MemberRegtype]))

    import spark.implicits._

    val rddObj = memberOBJ.map(e => {
      var rn = e.regsource match {
        //1.PC  2.MOBILE  3.APP   4.WECHAT
        case "1" => "PC"
        case "2" => "MOBILE"
        case "3" => "APP"
        case _ => "WECHAT"
      }
      e.regsourcename = rn
      e
    })
    import spark.implicits._
    val df1: DataFrame = rddObj.toDF()

    df1.createOrReplaceTempView("mreg")

    spark.sql("use dwd")//选中库
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(
      """
        |insert into dwd.dwd_member_regtype
        |select
        | uid,appkey,appregurl,bdp_uuid,createtime,isranreg,regsource,regsourcename,websiteid,
        | dt,dn
        |from mreg
      """.stripMargin)

    spark.close()
  }
}

case class MemberRegtype(
uid :Int,
appkey :String,
appregurl :String,
bdp_uuid :String,
createtime :String,
isranreg :String,
regsource :String,
var regsourcename :String,
websiteid :Int,
dt :String,
dn :String
)