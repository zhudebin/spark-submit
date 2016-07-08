package com.zmyuan.spark.submit

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.math._

/**
  * Created by zhudebin on 16/7/8.
  */
object DataframeGroupDemo {

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("test")

    val spark = SparkSession
    .builder
      .config(conf)
    .appName("Spark group")
    .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val rdd:RDD[Row] = spark.sparkContext.parallelize(1 until 1000).map{
      i => Row(i, s"name$i", i % 10)
    }

    val schema = StructType(Seq(StructField("id", IntegerType, false),
      StructField("name", StringType, false), StructField("age", IntegerType, false)))

    val df = spark.createDataFrame(rdd, schema)

//    df.groupBy("age").agg($"age", max("name"), sum("id")).collect().foreach(println _)

    val aggDf = df.groupBy("age").agg(Map("name" -> "max", "id" -> "sum"))
      aggDf.collect().foreach(println _)
    println("--------- schema ---------")
    aggDf.schema.foreach(println _)

    spark.stop()

  }

}
