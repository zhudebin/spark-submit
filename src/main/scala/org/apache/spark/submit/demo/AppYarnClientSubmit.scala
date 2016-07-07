package org.apache.spark.submit.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

/**
  * Created by zhudebin on 16/7/7.
  */
object AppYarnClientSubmit {

  def main(a: Array[String]) {

    //    val conf = new
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1g")
      .set("spark.submit.deployMode", "client")
      .setAppName("datashire")
      .set("spark.executor.instances", "0")
        .set("spark.dynamicAllocation.enabled", "true")
      //{"spark.shuffle.service.enabled":"true",
      // "spark.dynamicAllocation.cachedExecutorIdleTimeout":"1200s",
      // "spark.dynamicAllocation.minExecutors":"2"}
        .set("spark.shuffle.service.enabled", "true")
        .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "20s")
        .set("spark.dynamicAllocation.minExecutors", "2")
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
      .set("spark.yarn.jars", "hdfs://ehadoop/user/spark/spark200-ds-jars/*")
      .set("spark.yarn.user.jar", "local://Users/zhudebin/Documents/iworkspace/opensource/spark-submit/out/artifacts/spark_submit_jar/spark-submit.jar")
      .set("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")
      .set("spark.executor.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")

    // 先实例化 context, 将会导致下面的enableHiveSupport不起作用
//    val sc = SparkContext.getOrCreate(conf)

//    val session = SparkSession.builder().sparkContext(sc).enableHiveSupport().getOrCreate()
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
