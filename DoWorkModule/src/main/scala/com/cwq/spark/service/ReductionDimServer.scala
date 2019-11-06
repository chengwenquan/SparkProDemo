package com.cwq.spark.service

import org.apache.spark.sql.{SaveMode, SparkSession}

object ReductionDimServer {

  def DwsQzChapter(spark :SparkSession,time :String):Unit={
    spark.sql(
      s"""
         |SELECT
         |t1.chapterid,
         |t1.chapterlistid,
         |t1.chaptername,
         |t1.sequence,
         |t1.showstatus,
         |t2.status,
         |t1.creator chapter_creator,
         |t1.createtime chapter_createtime,
         |t1.courseid chapter_courseid,
         |t1.chapternum ,
         |t2.chapterallnum,
         |t1.outchapterid,
         |t2.chapterlistname,
         |t3.pointid,
         |t4.questionid ,
         |t4.questype ,
         |t3.pointname,
         |t3.pointyear,
         |t3.chapter,
         |t3.excisenum,
         |t3.pointlistid,
         |t3.pointdescribe,
         |t3.pointlevel,
         |t3.typelist,
         |t3.score point_score,
         |t3.thought,
         |t3.remid ,
         |t3.pointnamelist,
         |t3.typelistids ,
         |t3.pointlist ,
         |t1.dt,
         |t2.dn
         |from
         |dwd.dwd_qz_chapter t1
         |inner join  dwd.dwd_qz_chapter_list  t2 on t1.chapterlistid = t2.chapterlistid and t1.dn = t2.dn
         |inner join  dwd.dwd_qz_point t3 on t1.chapterid=t3.chapterid and t1.dn=t3.dn
         |inner join  dwd.dwd_qz_point_question t4 on t3.pointid=t4.pointid and t3.dn = t4.dn
         |where t1.dt='$time'
      """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_chapter")
  }

  def DwsQzMajor(spark :SparkSession,time :String):Unit={
    spark.sql(
      s"""
        |SELECT
        |t1.majorid,
        |t1.businessid,
        |t1.siteid,
        |t1.majorname,
        |t1.shortname,
        |t1.status,
        |t1.sequence,
        |t1.creator major_creator,
        |t1.createtime major_cretetime,
        |t3.businessname,
        |t2.sitename,
        |t2.domain,
        |t2.multicastserver,
        |t2.templateserver,
        |t2.multicastgateway,
        |t2.multicastport,
        |t1.dt,
        |t1.dn
        |from dwd.dwd_qz_major t1 inner join  dwd.dwd_qz_website t2 on t1.siteid = t2.siteid and t1.dn = t2.dn
        |inner join dwd.dwd_qz_business t3 on t1.businessid = t3.businessid and t1.dn=t3.dn
        |where t1.dt='$time'
      """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_major")
  }

  def DwsQzCourse(spark:SparkSession,time:String): Unit ={
    spark.sql(
      s"""
        |SELECT
        |t1.sitecourseid,
        |t1.siteid,
        |t1.courseid ,
        |t1.sitecoursename,
        |t1.coursechapter,
        |t1.sequence,
        |t1.status,
        |t1.creator sitecourse_creator,
        |t1.createtime sitecourse_createtime,
        |t1.helppaperstatus,
        |t1.servertype,
        |t1.boardid,
        |t1.showstatus,
        |t2.majorid ,
        |t2.coursename,
        |t2.isadvc ,
        |t2.chapterlistid,
        |t2.pointlistid,
        |t3.courseeduid,
        |t3.edusubjectid,
        |t1.dt,
        |t1.dn
        |from dwd.dwd_qz_site_course t1 inner join  dwd.dwd_qz_course t2 on t1.courseid=t2.courseid and t1.dn=t2.dn
        |inner join dwd.dwd_qz_course_edusubject t3 on t1.courseid=t3.courseid and t1.dn=t2.dn
        |where t1.dt=$time
      """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_course")
  }

  def DwsQzPaper(spark:SparkSession,time:String): Unit ={
    spark.sql(
      s"""
        |SELECT
        |t1.paperviewid,
        |t1.paperid ,
        |t1.paperviewname,
        |t1.paperparam,
        |t1.openstatus,
        |t1.explainurl,
        |t1.iscontest,
        |t1.contesttime,
        |t1.conteststarttime,
        |t1.contestendtime,
        |t1.contesttimelimit,
        |t1.dayiid,
        |t1.status,
        |t1.creator paper_view_creator,
        |t1.createtime paper_view_createtime,
        |t1.paperviewcatid,
        |t1.modifystatus,
        |t1.description,
        |t1.paperuse,
        |t1.paperdifficult,
        |t1.testreport,
        |t1.paperuseshow,
        |t2.centerid ,
        |t2.sequence,
        |t3.centername,
        |t3.centeryear,
        |t3.centertype,
        |t3.provideuser,
        |t3.centerviewtype,
        |t3.stage,
        |t4.papercatid,
        |t4.courseid,
        |t4.paperyear ,
        |t4.suitnum ,
        |t4.papername ,
        |t4.totalscore ,
        |t4.chapterid  ,
        |t4.chapterlistid ,
        |t1.dt,
        |t1.dn
        |from dwd.dwd_qz_paper_view t1 left join dwd.dwd_qz_center_paper t2 on t1.paperviewid=t2.paperviewid and t1.dn=t2.dn
        |left join dwd.dwd_qz_center t3 on t2.centerid=t3.centerid and t2.dn=t3.dn
        |inner join dwd.dwd_qz_paper t4 on t1.paperid=t4.paperid and t1.dn=t4.dn
        |where t1.dt='$time'
      """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_paper")
  }

  def DwsQzQuestion(spark :SparkSession,time:String): Unit ={
    spark.sql(
      s"""
        |SELECT 
        |t1.questionid,
        |t1.parentid,
        |t1.questypeid,
        |t1.quesviewtype,
        |t1.content,
        |t1.answer,
        |t1.analysis,
        |t1.limitminute,
        |t1.score ,
        |t1.splitscore,
        |t1.status,
        |t1.optnum,
        |t1.lecture,
        |t1.creator,
        |t1.createtime,
        |t1.modifystatus,
        |t1.attanswer,
        |t1.questag,
        |t1.vanalysisaddr,
        |t1.difficulty,
        |t1.quesskill,
        |t1.vdeoaddr,
        |t2.viewtypename,
        |t2.description,
        |t2.papertypename ,
        |t2.splitscoretype,
        |t1.dt,
        |t1.dn
        |from dwd.dwd_qz_question t1 left join dwd.dwd_qz_question_type t2 on t1.questypeid=t2.questypeid and t1.dn=t2.dn
        |where t1.dt='$time'
      """.stripMargin).coalesce(2).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_question")
  }

  def DwsUserPaperDetail(spark :SparkSession,time:String): Unit ={
    spark.sql(
      s"""
        |insert into dws.dws_user_paper_detail
        |select
        |a.userid,
        |c.courseid,
        |a.questionid,
        |a.useranswer,
        |a.istrue,
        |a.lasttime,
        |a.opertype,
        |a.paperid,
        |a.spendtime,
        |a.chapterid,
        |b.chaptername,
        |b.chapternum,
        |b.chapterallnum,
        |b.outchapterid,
        |b.chapterlistname,
        |b.pointid,
        |b.questype,
        |b.pointyear,
        |b.chapter,
        |b.pointname,
        |b.excisenum,
        |b.pointdescribe,
        |b.pointlevel,
        |b.typelist,
        |b.point_score,
        |b.thought,
        |b.remid,
        |b.pointnamelist,
        |b.typelistids,
        |b.pointlist,
        |c.sitecourseid,
        |c.siteid,
        |c.sitecoursename,
        |c.coursechapter,
        |c.`sequence`,
        |c.status,
        |c.sitecourse_creator,
        |c.sitecourse_createtime,
        |c.servertype,
        |c.helppaperstatus,
        |c.boardid,
        |c.showstatus,
        |c.majorid,
        |c.coursename,
        |c.isadvc,
        |c.chapterlistid,
        |c.pointlistid,
        |c.courseeduid,
        |c.edusubjectid,
        |d.businessid,
        |d.majorname,
        |d.shortname,
        |d.status,
        |d.`sequence`,
        |d.major_creator,
        |d.major_createtime,
        |d.businessname,
        |d.sitename,
        |d.`domain`,
        |d.multicastserver,
        |d.templateserver,
        |d.multicastgateway,
        |d.multicastport,
        |e.paperviewid,
        |e.paperviewname,
        |e.paperparam,
        |e.openstatus,
        |e.explainurl,
        |e.iscontest,
        |e.contesttime,
        |e.conteststarttime,
        |e.contestendtime,
        |e.contesttimelimit,
        |e.dayiid,
        |e.status,
        |e.paper_view_creator,
        |e.paper_view_createtime,
        |e.paperviewcatid,
        |e.modifystatus,
        |e.description,
        |e.paperuse,
        |e.testreport,
        |e.centerid,
        |e.`sequence`,
        |e.centername,
        |e.centeryear,
        |e.centertype,
        |e.provideuser,
        |e.centerviewtype,
        |e.stage,
        |e.papercatid,
        |e.paperyear,
        |e.suitnum,
        |e.papername,
        |e.totalscore,
        |f.parentid,
        |f.questypeid,
        |f.quesviewtype,
        |f.content,
        |f.answer,
        |f.analysis,
        |f.limitminute,
        |f.score,
        |f.splitscore,
        |f.lecture,
        |f.creator,
        |f.createtime,
        |f.modifystatus,
        |f.attanswer,
        |f.questag,
        |f.vanalysisaddr,
        |f.difficulty,
        |f.quesskill,
        |f.vdeoaddr,
        |f.description,
        |f.splitscoretype,
        |a.question_answer,
        |a.dt,
        |a.dn
        |from dwd.dwd_qz_member_paper_question a join dws.dws_qz_chapter b
        |on a.chapterid = b.chapterid and a.dn = b.dn
        |join dws.dws_qz_course c
        |on a.sitecourseid = c.sitecourseid and a.dn = c.dn
        |join dws.dws_qz_major d
        |on a.majorid = d.majorid and a.dn = d.dn
        |join dws.dws_qz_paper e
        |on a.paperviewid = e.paperviewid and a.dn = e.dn
        |join dws.dws_qz_question f
        |on a.questionid = f.questionid and a.dn = f.dn
        |where a.dt = ${time}
      """.stripMargin)
  }

  def DwsSelectUserPaperDetail(spark: SparkSession,time:String): Unit ={
    spark.sql(
      s"""
        |select *  from dws.dws_user_paper_detail where dt = ${time} limit 1
      """.stripMargin).show()
  }

}
