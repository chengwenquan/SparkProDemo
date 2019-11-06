package com.cwq.spark.service

import com.alibaba.fastjson.JSON
import com.cwq.spark.bean._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object ETLAndImportDataServer {

  /**
    * 所属行业数据表
    * @param spark
    * @param sc
    */
  def ETLQzBusiness(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzBusiness.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzBusiness] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzBusiness]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_business")
  }

  /**
    * 主题数据表
    * @param spark
    * @param sc
    */
  def ETLQzCenter(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzCenter.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzCenter] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzCenter]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_center")
  }

  /**
    * 试卷主题关联数据表
    * @param spark
    * @param sc
    */
  def ETLQzCenterPaper(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzCenterPaper.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzCenterPaper] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzCenterPaper]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_center_paper")
  }

  /**
    * 章节数据表
    * @param spark
    * @param sc
    */
  def ETLQzChapter(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzChapter.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzChapter] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzChapter]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_chapter")
  }

  /**
    * 章节列表数据表
    * @param spark
    * @param sc
    */
  def ETLQzChapterList(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzChapterList.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzChapterList] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzChapterList]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_chapter_list")
  }

  /**
    * 题库课程数据表
    * @param spark
    * @param sc
    */
  def ETLQzCourse(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzCourse.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzCourse] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzCourse]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_course")
  }

  /**
    * 课程辅导数据表
    * @param spark
    * @param sc
    */
  def ETLQzCourseEdusubject(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzCourseEduSubject.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzCourseEdusubject] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzCourseEdusubject]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_course_edusubject")
  }

  /**
    * 主修数据表
    * @param spark
    * @param sc
    */
  def ETLQzMajor(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzMajor.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzMajor] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzMajor]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_major")
  }

  /**
    * 学员做题详情数据表
    * @param spark
    * @param sc
    */
  def ETLQzMemberPaperQuestion(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzMemberPaperQuestion.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzMemberPaperQuestion] = sourceStr
      .map(e => {
        val question: QzMemberPaperQuestion = JSON.parseObject(e,classOf[QzMemberPaperQuestion])
        question.score = BigDecimal.apply(question.score).toString()
        question
      })
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_member_paper_question")
  }

  /**
    * 做题试卷日志数据表
    * @param spark
    * @param sc
    */
  def ETLQzPaper(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzPaper.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzPaper] = sourceStr
      .map(e => {
        val paper: QzPaper = JSON.parseObject(e,classOf[QzPaper])
        paper.totalscore = BigDecimal.apply(paper.totalscore).toString()
        paper
      })
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_paper")
  }

  /**
    * 试卷视图数据表
    * @param spark
    * @param sc
    */
  def ETLQzPaperView(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzPaperView.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzPaperView] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzPaperView]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_paper_view")
  }

  /**
    * 知识点数据日志表
    * @param spark
    * @param sc
    */
  def ETLQzPoint(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzPoint.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzPoint] = sourceStr
      .map(e => {
        val point: QzPoint = JSON.parseObject(e,classOf[QzPoint])
        point.score = BigDecimal.apply(point.score).toString()
        point
      })
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_point")
  }

  /**
    * 做题知识点关联数据表
    * @param spark
    * @param sc
    */
  def ETLQzPointQuestion(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzPointQuestion.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzPointQuestion] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzPointQuestion]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_point_question")
  }

  /**
    * 做题日志数据表
    * @param spark
    * @param sc
    */
  def ETLQzQuestion(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzQuestion.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzQuestion] = sourceStr
      .map(e => {
        val qsquestion: QzQuestion = JSON.parseObject(e,classOf[QzQuestion])
        qsquestion.score = BigDecimal.apply(qsquestion.score).toString()
        qsquestion
      })
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_question")
  }

  /**
    * 题目类型数据表
    * @param spark
    * @param sc
    */
  def ETLQzQuestionType(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzQuestionType.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzQuestionType] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzQuestionType]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_question_type")
  }

  /**
    * 网站课程日志数据表
    * @param spark
    * @param sc
    */
  def ETLQzSiteCourse(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzSiteCourse.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzSiteCourse] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzSiteCourse]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_site_course")
  }

  /**
    * 做题网站日志数据表
    * @param spark
    * @param sc
    */
  def ETLQzWebsite(spark: SparkSession,sc:SparkContext): Unit ={
    //从文件中查询出来的源数据
    val sourceStr: RDD[String] = sc.textFile("/user/sourceFile/ods/QzWebsite.log")
    //将数据封装成对象
    val obj_RDD: RDD[QzWebsite] = sourceStr
      .map(e => JSON.parseObject(e,classOf[QzWebsite]))
    import spark.implicits._
    obj_RDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_website")
  }

}
