package com.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-25 8:40
 */
object day03Test1 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(9, 8, 7, 6, 5, 4), 2)

    // 交集 intersection
    rdd1.intersection(rdd2).collect().foreach(println)

    // 并集 union
    rdd1.union(rdd2).collect().foreach(println)

    // 差集
    rdd1.subtract(rdd2).collect().foreach(println)

    // 拉链
    rdd1.zip(rdd2).collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
