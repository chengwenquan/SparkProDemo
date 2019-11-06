package com.cwq.spark.controller

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * DWS层统计每天各个网站的会员注册情况
  */
object MemberWideTableDWS {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("baseadlog").master("local[*]")
      .enableHiveSupport() // 支持hive
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val dt = "20190722"
    spark.sql(
      s"""
        |insert into dws.dws_member
        |select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel,a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip,a.zipcode,b.appkey,b.appregurl,b.bdp_uuid,
        |b.createtime as reg_createtime,b.isranreg,b.regsource,b.regsourcename,c.adname,d.siteid,
        |d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime,d.creator as site_creator,
        |f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time,
        |f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free,f.next_level as vip_next_level,
        |f.operator as vip_operator,a.dt,a.dn
        |from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid and a.dn=b.dn
        |left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn
        |left join dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn
        |left join (select uid ,sum(cast(paymoney as decimal(10,4))) as paymoney,siteid ,vip_id ,dt ,dn from dwd.dwd_pcentermempaymoney where dt='$dt' group by uid,siteid,vip_id,dt,dn) e on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='$dt'
      """.stripMargin)

    spark.close()
  }
}