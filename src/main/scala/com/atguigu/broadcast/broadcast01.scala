package com.atguigu.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-27 11:40
 */
object broadcast01 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"), 4)
    val list = "String"

    val warn: Broadcast[String] = sc.broadcast(list)

    val filter: RDD[String] = rdd.filter {
      log => log.contains(warn.value)
    }

    filter.foreach(println)
    // 4. 关闭sc
    sc.stop()
  }
}
