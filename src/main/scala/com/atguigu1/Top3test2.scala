package com.atguigu1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 20:28
 */

object Top3test2 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/agent.log")

    rdd.map {
      line => {
        val strings: Array[String] = line.split(" ")
        (strings(1) + "-" + strings(4), 1)
      }
    }.reduceByKey(_ + _)
      .map {
        case (key, value) => {
          val strings: Array[String] = key.split("-")
          (strings(0), (strings(1), value))
        }
      }.groupByKey()
      .map {
        case (key, value) => {
          val tuples: List[(String, Int)] = value.toList.sortWith {
            (lift, right) => {
              lift._2 > right._2
            }
          }.take(3)
          (key,tuples)
        }
      }.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
