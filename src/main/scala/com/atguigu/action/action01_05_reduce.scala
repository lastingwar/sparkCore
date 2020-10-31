package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-25 11:30
 */
object action01_05_reduce {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    val value: Int = rdd.reduce(_ + _)

    println(value)
    rdd .collect().foreach(println)

    println(rdd.count())
    println(rdd.first())

    println(rdd.take(2).toBuffer)
    // 4. 关闭sc
    sc.stop()
  }
}
