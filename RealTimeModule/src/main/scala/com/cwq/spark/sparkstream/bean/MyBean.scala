package com.cwq.spark.sparkstream.bean

object MyBean {

  def main(args: Array[String]): Unit = {
      var str =
        """
          |408646	1	2019-07-16 16:01:57
        """.stripMargin
      val sp: Array[String] = str.split("\t")
      println(sp(2))
  }

}

//0	1	2019-07-16 16:01:55
//用户id     平台id  1:PC  2:APP   3:Ohter       创建时间

case class UserReginer(userId:String,appId:Int,createTime:String)

//{"app_id":"1","device_id":"102","distinct_id":"5fa401c8-dd45-4425-b8c6-700f9f74c532","event_name":"-","ip":"121.76.152.135","last_event_name":"-","last_page_id":"0","next_event_name":"-","next_page_id":"2","page_id":"1","server_time":"-","uid":"245494"}
case class Page(
                 app_id:String,
                 device_id:String,
                 distinct_id:String,
                 event_name:String,
                 ip:String,
                 last_event_name:String,
                 last_page_id:String,
                 next_event_name:String,
                 next_page_id:String,
                 page_id:String,
                 server_time:String,
                 uid:String
               )