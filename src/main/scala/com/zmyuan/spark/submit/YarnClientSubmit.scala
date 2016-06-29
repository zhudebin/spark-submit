package com.zmyuan.spark.submit

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.StdIn

/**
  * Created by zhudebin on 16/6/29.
  */
object YarnClientSubmit {

  def main(args: Array[String]) {

//    val conf = new
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1g")
      .set("spark.submit.deployMode", "client")
      .setAppName("datashire")
      .set("spark.executor.instances", "2")
      .set("spark.executor.memory", "2g")
      .set("spark.executor.cores", "1")
      .set("spark.yarn.queue", "spark-submit")
//      .set("spark.executor.extraLibraryPath", "")
//      .set("spark.executor.extraClassPath", "")
      .set("spark.master", "yarn")
      .set("spark.submit.deployMode", "client")
          .set("spark.executor.userClassPathFirst","true")
          .set("spark.driver.userClassPathFirst","true")
      .set("spark.scheduler.mode", "FAIR")
//      .set("spark.shuffle.manager", "hash")
      .set("spark.sql.shuffle.partitions", "200")
      .set("spark.yarn.jars", "hdfs://ehadoop/user/spark/spark-2.0/jars/*")
        .set("spark.yarn.user.jar", "local://Users/zhudebin/Documents/iworkspace/opensource/spark-submit/out/artifacts/spark_submit_jar/spark-submit.jar")
      .set("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")
      .set("spark.executor.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")

    val sc = new SparkContext(conf)

    val session = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

//    session.
    session.sql("show databases").collect().foreach(println)

    do{
      val sql = StdIn.readLine()
      println(s"=== sql ===: $sql == result:")
      session.sql(sql).collect().foreach(println)
    } while(true)


  }


}
