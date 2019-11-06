package com.cwq.spark.controller

import org.apache.spark.sql.{KeyValueGroupedDataset, SaveMode, SparkSession}

/**
  * 拉链表，每天更新会员的支付金额，并修改跟新日期
  *
  * uid paymoney start_time end_time
  * 1   10        2019-10-01 2019-10-02
  * 2   10        2019-10-01 9999-12-31 最终
  * 1   10+10=20  2019-10-02 2019-10-03
  * 1   20+10=30  2019-10-03 9999-12-31 最终
  */
object PayMoneyByMemberZipperDWS {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("baseadlog").master("local[*]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val time = "20190722"

    //查询当天增量数据
     spark.sql(
       s"""
          |select z.uid,z.paymoney,z.vip_level,z.start_time,z.end_time,z.dn from dws.dws_member_zipper z
          |left join
          |(
          |select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level,
          |from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,
          |'9999-12-31' as end_time,first(a.dn) as dn
          |from dwd.dwd_pcentermempaymoney a join dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn
          |where a.dt='$time' group by uid
          |) nd on z.uid=nd.uid and z.dn=nd.dn where nd.uid is null
          |union
          |select z.uid,(z.paymoney+nd.paymoney) paymoney,z.vip_level,z.start_time,nd.start_time,z.dn from dws.dws_member_zipper z
          |left join
          |(
          |select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level,
          |from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,
          |'9999-12-31' as end_time,first(a.dn) as dn
          |from dwd.dwd_pcentermempaymoney a join dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn
          |where a.dt='$time' group by uid
          |) nd on z.uid=nd.uid and z.dn=nd.dn where nd.uid is not null
          |union
          |select nd.uid,nd.paymoney,nd.vip_level,nd.start_time,nd.end_time,nd.dn from
          |(
          |select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level,
          |from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,
          |'9999-12-31' as end_time,first(a.dn) as dn
          |from dwd.dwd_pcentermempaymoney a join dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn
          |where a.dt='$time' group by uid
          |) nd left join dws.dws_member_zipper z on nd.uid=z.uid and nd.dn=z.dn where z.uid is null
          |
        """.stripMargin).show()
    spark.close()
  }
}

object PayMoneyByMemberZipperDWS1 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("baseadlog").master("local[*]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val time = "20190722"
    import spark.implicits._
    //查询当天增量数据
    val dayResult = spark.sql(s"select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,b.vip_level," +
      s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,a.dn " +
      " from dwd.dwd_pcentermempaymoney a join " +
      s"dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn where a.dt='$time' group by uid,b.vip_level,a.dn").as[MemberZipper]
    //获取历史数据
    val historyResult = spark.sql("select * from dws.dws_member_zipper").as[MemberZipper]
    //合并历史并按uid和dn进行分组
    val groups: KeyValueGroupedDataset[String, MemberZipper] = dayResult.union(historyResult).groupByKey(e => e.uid+"_"+e.dn)

    groups.mapGroups{
      case (key,iters) => {
        val keys: Array[String] = key.split("_")
        val uid = keys(0)
        val dn = keys(1)

        val list = iters.toList.sortBy(item => item.start_time) //对开始时间进行排序
        if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) { //说明历史表中有数据
          val oldLastModel = list(list.size - 2)//获取倒数第二条数据
          val lastModel = list(list.size - 1)//获取最后一条数据

          oldLastModel.end_time = lastModel.start_time//将到最后一条的开始时间作为倒数第二条的结束时间
          //累加付费金额，并放到最后一条数据上
          lastModel.paymoney = (BigDecimal.apply(lastModel.paymoney) + BigDecimal(oldLastModel.paymoney)).toString()
        }
        MemberZipperResult(list)
      }
    }
      .flatMap(_.list)
      .coalesce(3)//可以减少小文件，如果可以越小越好
      .write.mode(SaveMode.Overwrite) //覆盖写入dws.dws_member_zipper
      .insertInto("dws.dws_member_zipper")
    spark.close()
  }
}

case class MemberZipper(
                         uid: Int,
                         var paymoney: String,
                         vip_level: String,
                         start_time: String,
                         var end_time: String,
                         dn: String
                       )

case class MemberZipperResult(list: List[MemberZipper])