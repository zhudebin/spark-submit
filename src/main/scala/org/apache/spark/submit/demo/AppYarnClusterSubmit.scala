package org.apache.spark.submit.demo

import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.spark.{SparkException, SparkConf}
import org.apache.spark.deploy.yarn.{Client, ClientArguments}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhudebin on 16/7/7.
  */
object AppYarnClusterSubmit {

  def main(a: Array[String]) {

    val argsArrayBuf = new ArrayBuffer[String]()

    // --class
//    argsArrayBuf.+=("--class").+=("com.zmyuan.spark.submit.SparkPi")


    val args = new ClientArguments(argsArrayBuf.toArray)

    args.userJar = "/Users/zhudebin/Documents/iworkspace/opensource/spark-submit/out/artifacts/spark_submit_jar/spark-submit.jar"
    args.userArgs = new ArrayBuffer[String]() += "100"
    args.userClass = "com.zmyuan.spark.submit.SparkPi"

    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1g")
      .setAppName("datashire")
      .set("spark.executor.instances", "3")
//      .set("spark.dynamicAllocation.enabled", "true")
      //{"spark.shuffle.service.enabled":"true",
      // "spark.dynamicAllocation.cachedExecutorIdleTimeout":"1200s",
      // "spark.dynamicAllocation.minExecutors":"2"}
      .set("spark.shuffle.service.enabled", "true")
//      .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "20s")
//      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.executor.memory", "2g")
      .set("spark.executor.cores", "1")
      .set("spark.yarn.queue", "spark-submit")
      //      .set("spark.executor.extraLibraryPath", "")
      //      .set("spark.executor.extraClassPath", "")
      .set("spark.master", "yarn")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.executor.userClassPathFirst","true")
      .set("spark.driver.userClassPathFirst","true")
      .set("spark.scheduler.mode", "FAIR")
      //      .set("spark.shuffle.manager", "hash")
      .set("spark.sql.shuffle.partitions", "200")
      .set("spark.yarn.jars", "hdfs://ehadoop/user/spark/spark200-ds-jars/*")
//      .set("spark.yarn.user.jar", "local://Users/zhudebin/Documents/iworkspace/opensource/spark-submit/out/artifacts/spark_submit_jar/spark-submit.jar")
      .set("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")
      .set("spark.executor.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")

    System.setProperty("SPARK_YARN_MODE", "true")

    val client = new Client(args, conf)

    val appId = client.submitApplication()

    do {
      val (state, _) = client.monitorApplication(appId, returnOnRunning = true) // blocking
      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        throw new SparkException("Yarn application has already ended! " +
          "It might have been killed or unable to launch application master.")
      }
      if (state == YarnApplicationState.RUNNING) {
        println(s"Application ${appId} has started running.")
      }
      Thread.sleep(2000l)
    } while (true)

  }

}
