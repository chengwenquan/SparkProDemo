package com.cwq.dsl.service

import com.cwq.dsl.dao.BaseSQLService
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object TestDSLDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val users = BaseSQLService.SelectUser(spark).where("id<7839")
    val depts = BaseSQLService.SelectDept(spark)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //select语法
   // users.select($"id",col("name").as("ename")).show() //取别名
   // users.select($"id",$"name".as("ename")).show()
   // users.selectExpr("id as eid","name","sal+200").show() //在字段上做运算

    //过滤 TODO 不能用
   // users.where($"name"==="张三10").select("id","name").show()

    //排序
    //单列排序
    //users.sort("sal").select("name","sal").show()//升序
    //users.sort($"sal".desc).select("name","sal").show()//降序
    //users.orderBy("sal").select("name","sal","deptno").show()
    //多列排序
    //users.sort($"deptno".asc,$"sal".desc).select("name","sal","deptno").show()//deptno升序，sal降序
    //users.orderBy($"deptno".asc,$"sal".desc).select("name","sal","deptno").show()//deptno升序，sal降序
    //分区区内排序
    //users.repartition(3).sortWithinPartitions("sal").select("name","sal","deptno").show()

    //分组
//    users.groupBy("deptno").sum("sal").show() //输出字段deptno、sum(sal)
//    users.groupBy("deptno").agg(sum("sal").as("sum_sal")).show()//输出字段deptno、sum_sal
//    users.groupBy("deptno").agg(sum("sal").as("sum_sal"),avg("sal").as("avg_sal")).show()

    //limit
    //users.limit(1).show()


    //join
    //users.join(depts,Seq("deptno")).show()//两张表join时如果条件字段相同可以将字段写入seq
    //users.join(depts,users("deptno")===depts("id"),"inner").show()//当两个表中都有相同的字段时可加上表名区分

    //窗口函数
//    val win: WindowSpec = Window.orderBy("sal")
//    users.select($"name",$"sal",row_number().over(win).as("rank")).show()
//
//    val win1: WindowSpec = Window.partitionBy("deptno").orderBy("sal")
//    users.select($"name",$"sal",$"deptno",row_number().over(win1).as("rank")).show()

    //增加一列
    users.withColumn("rownum",$"sal"+100).show()

    spark.close()
  }

}
