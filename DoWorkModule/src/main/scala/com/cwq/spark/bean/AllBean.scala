package com.cwq.spark.bean

class AllBean {
}

/**
  * 所属行业数据表
  */
case class QzBusiness(
                       businessid: Int, //行业id
                       businessname: String,//行业名称
                       sequence: String,
                       status: String,
                       creator: String,//创建者
                       createtime: String,//创建时间
                       siteid: Int,//所属网站id
                       dt: String,
                       dn: String
                     )

/**
  * 主题数据表
  */
case class QzCenter(
                     centerid: Int,
                     centername: String,
                     centeryear: String,
                     centertype: String,
                     openstatus: String,
                     centerparam: String,
                     description: String,
                     creator: String,
                     createtime: String,
                     sequence: String,
                     provideuser: String,
                     centerviewtype: String,
                     stage: String,
                     dt: String,
                     dn: String
                   )

/**
  * 试卷主题关联数据表
  */
case class QzCenterPaper(
                          paperviewid: Int,
                          centerid: Int,
                          openstatus: String,
                          sequence: String,
                          creator: String,
                          createtime: String,
                          dt: String,
                          dn: String
                        )

/**
  * 章节数据表
  */
case class QzChapter (
                       chapterid: Int,
                       chapterlistid: Int,
                       chaptername: String,
                       sequence: String,
                       showstatus: String,
                       creator: String,
                       createtime: String,
                       courseid: Int,
                       chapternum: Int,
                       outchapterid: Int,
                       dt: String,
                       dn: String
                     )

/**
  *章节列表数据表
  */
case class QzChapterList(
                          chapterlistid: Int,
                          chapterlistname: String,
                          courseid: Int,
                          chapterallnum: Int,
                          sequence: String,
                          status: String,
                          creator: String,
                          createtime: String,
                          dt: String,
                          dn: String
                        )
/**
  *题库课程数据表
  */
case class QzCourse (
                      courseid: Int,
                      majorid: Int,
                      coursename: String,
                      coursechapter: String,
                      sequence: String,
                      isadvc: String,
                      creator: String,
                      createtime: String,
                      status: String,
                      chapterlistid: Int,
                      pointlistid: Int,
                      dt: String,
                      dn: String
                    )

/**
  * 课程辅导数据表
  */
case class QzCourseEdusubject (
                                courseeduid: Int ,
                                edusubjectid: Int ,
                                courseid: Int ,
                                creator: String,
                                createtime: String,
                                majorid: Int,
                                dt: String,
                                dn: String
                              )

/**
  *主修数据表
  */
case class QzMajor(
                    majorid: Int,
                    businessid: Int,
                    siteid: Int,
                    majorname: String,
                    shortname: String,
                    status: String,
                    sequence: String,
                    creator: String,
                    createtime: String,
                    column_sitetype: String,
                    dt: String,
                    dn: String
                  )
/**
  * 学员做题详情数据表
  */
case class QzMemberPaperQuestion(
                                  userid: Int,
                                  paperviewid: Int,
                                  chapterid: Int,
                                  sitecourseid: Int,
                                  questionid: Int,
                                  majorid: Int,
                                  useranswer: String,
                                  istrue: String,
                                  lasttime: String,
                                  opertype: String,
                                  paperid: Int,
                                  spendtime: Int,
                                  var score: String,
                                  question_answer: Int,
                                  dt: String,
                                  dn: String
                                )

/**
  * 做题试卷日志数据表
  */
case class QzPaper(
                    paperid: Int,
                    papercatid: Int,
                    courseid: Int,
                    paperyear: String,
                    chapter: String,
                    suitnum: String,
                    papername: String,
                    status: String,
                    creator: String,
                    createtime: String,
                    var totalscore: String,
                    chapterid: Int,
                    chapterlistid: Int,
                    dt: String,
                    dn: String
                  )
/**
  * 试卷视图数据表
  */
case class QzPaperView(
                        paperviewid: Int,
                        paperid: Int,
                        paperviewname: String,
                        paperparam: String,
                        openstatus: String,
                        explainurl: String,
                        iscontest: String,
                        contesttime: String,
                        conteststarttime: String,
                        contestendtime: String,
                        contesttimelimit: String,
                        dayiid: Int,
                        status: String,
                        creator: String,
                        createtime: String,
                        paperviewcatid: Int,
                        modifystatus: String,
                        description: String,
                        papertype: String,
                        downurl: String,
                        paperuse: String,
                        paperdifficult: String,
                        testreport: String,
                        paperuseshow: String,
                        dt: String,
                        dn: String
                      )
/**
  * 知识点数据日志表
  */
case class QzPoint(
                    pointid: Int,
                    courseid: Int,
                    pointname: String,
                    pointyear: String,
                    chapter: String,
                    creator: String,
                    createtme: String,
                    status: String,
                    modifystatus: String,
                    excisenum: Int,
                    pointlistid: Int,
                    chapterid: Int,
                    sequece: String,
                    pointdescribe: String,
                    pointlevel: String,
                    typelist: String,
                    var score: String,
                    thought: String,
                    remid: String,
                    pointnamelist: String,
                    typelistids: String,
                    pointlist: String,
                    dt: String,
                    dn: String
                  )
/**
  * 做题知识点关联数据表
  */
case class QzPointQuestion(
                            pointid: Int,
                            questionid: Int,
                            questype: Int,
                            creator: String,
                            createtime: String,
                            dt: String,
                            dn: String
                          )
/**
  * 做题日志数据表
  */
case class QzQuestion(
                       questionid: Int,
                       parentid: Int,
                       questypeid: Int,
                       quesviewtype: Int,
                       content: String,
                       answer: String,
                       analysis: String,
                       limitminute: String,
                       var score: String,
                       splitscore: String,
                       status: String,
                       optnum: Int,
                       lecture: String,
                       creator: String,
                       createtime: String,
                       modifystatus: String,
                       attanswer: String,
                       questag: String,
                       vanalysisaddr: String,
                       difficulty: String,
                       quesskill: String,
                       vdeoaddr: String,
                       dt: String,
                       dn: String
                     )
/**
  * 题目类型数据表
  */
case class QzQuestionType(
                           quesviewtype: Int,
                           viewtypename: String,
                           questypeid: Int,
                           description: String,
                           status: String,
                           creator: String,
                           createtime: String,
                           papertypename: String,
                           sequence: String,
                           remark: String,
                           splitscoretype: String,
                           dt: String,
                           dn: String
                         )

/**
  *网站课程日志数据表
  */
case class QzSiteCourse (
                          sitecourseid: Int,
                          siteid: Int ,
                          courseid: Int ,
                          sitecoursename: String ,
                          coursechapter: String ,
                          sequence: String,
                          status: String,
                          creator: String,
                          createtime: String,
                          helppaperstatus: String,
                          servertype: String,
                          boardid: Int,
                          showstatus: String,
                          dt: String,
                          dn: String
                        )
/**
  * 做题网站日志数据表
  */
case class QzWebsite (
                       siteid: Int ,
                       sitename: String ,
                       domain: String,
                       sequence: String,
                       multicastserver: String,
                       templateserver: String,
                       status: String,
                       creator: String,
                       createtime: String,
                       multicastgateway: String,
                       multicastport: String,
                       dt: String,
                       dn: String
                     )
