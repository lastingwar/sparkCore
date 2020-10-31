package com.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-25 8:57
 */

object day03Top3 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/agent.log")

    rdd.map {
      line => {
        val strings: Array[String] = line.split(" ")
        // 省份-广告,1
        (strings(1) + "-" + strings(4), 1)
      }
    }
      .reduceByKey(_ + _) //省份-广告,sum
      .map {
        case (key, v) =>
          val strings: Array[String] = key.split("-")
          //省份,(广告,sum)
          (strings(0), (strings(1), v))
      }.groupByKey() //省份分组
      .map {
        case (key, list) =>
          val tuples: List[(String, Int)] = list.toList.sortWith {
            (lift, right) =>
              lift._2 > right._2
          }.take(3) // 点击数排序取前三
          (key,tuples) // 返回 (省份,(广告编号,点击数))
      }.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
