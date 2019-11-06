package com.cwq.spark.etl

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求1：必须使用Spark进行数据清洗，对用户名、手机号、密码进行脱敏处理，
  * 并使用Spark将数据导入到dwd层hive表中
  *
  * 清洗规则 用户名：王XX   手机号：137*****789  密码直接替换成******
  *
  * 表格 member
  */
object UserEtl {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("ETL")
      .master("local[2]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val rdd_str: RDD[String] = sc.textFile("/user/sourceFile/ods/member.log")

    //将数据封装成对象
    val memberOBJ= rdd_str.map(JSON.parseObject(_, classOf[MemberLog]))

    val rddObj: RDD[MemberLog] = memberOBJ.map(e => {
      //姓名脱敏
      val fullname: String = e.fullname.substring(0, 1) + "**"
      e.fullname = fullname
      e.password = "*********"
      //手机号脱敏
      val phone: String = e.phone
      val start: String = phone.substring(0, 3)
      val end: String = phone.substring(phone.length - 3)
      val newphone: String = start + "*****" + end
      e.phone = newphone
      e
    })

    import spark.implicits._
    val df1: DataFrame = rddObj.toDF()

    df1.createOrReplaceTempView("memberlog")

    spark.sql("use dwd")//选中库
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(
      """
        |insert into dwd.dwd_member
        |select
        |uid,
        |ad_id ,
        |birthday ,
        |email ,
        |fullname ,
        |iconurl ,
        |lastlogin ,
        |mailaddr ,
        |memberlevel ,
        |password ,
        |paymoney ,
        |phone ,
        |qq ,
        |register ,
        |regupdatetime ,
        |unitname ,
        |userip ,
        |zipcode ,
        |dt ,
        |dn
        |from memberlog
      """.stripMargin)


    spark.close()
  }
}


//会员
case class MemberLog (var uid :Int,
                      var ad_id :Int,
                      var birthday :String,
                      var email :String,
                      var fullname :String,
                      var iconurl :String,
                      var lastlogin :String,
                      var mailaddr :String,
                      var memberlevel :String,
                      var password :String,
                      var paymoney :String,
                      var phone :String,
                      var qq :String,
                      var register :String,
                      var regupdatetime :String,
                      var unitname :String,
                      var userip :String,
                      var zipcode :String,
                      var dt :String,
                      var dn :String
                     )



