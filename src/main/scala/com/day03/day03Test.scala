package com.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-25 8:28
 */
object day03Test {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    val rdd1: RDD[String] = sc.makeRDD(List("hello world", "word count", "xxxxwwww", "qqq sss"))

    // 1.map RDD中的数+1
    rdd.map(_ + 1).collect().foreach(println)

    // 2.mappartition RDD中的数+1
    rdd.mapPartitions(_.map(_ + 1)).collect().foreach(println)

    // 3.mappartitionWithIndex 输入RDD中的数,输出(分区号,元素)
    rdd.mapPartitionsWithIndex((nums, list) => {
      list.map((nums, _))
    }).collect().foreach(println)

    // 4.groupBy 按照数的奇偶分区
    rdd.groupBy(_ % 2).collect().foreach(println)

    // 5.glom 分区合并为一个数组
    rdd.glom().collect().foreach(println)

    // 6.filter 过滤掉偶数
    rdd.filter(_ % 2 == 0).collect().foreach(println)

    // 7.flatMap 按照空格进行切分
    rdd1.flatMap(_.split(" ")).collect().foreach(println)

    // 8.coalesce 缩减分区
    rdd.coalesce(2).collect().foreach(println)

    // 9.repartition 重新分区
    rdd.repartition(5).collect().foreach(println)

    // 10.sortBy 排序
    rdd.sortBy(key=>key).collect().foreach(println)

    // 11. sample
    rdd.sample(false,0.5).collect().foreach(println)

    // 12.distinct 去重
    rdd.distinct().collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
