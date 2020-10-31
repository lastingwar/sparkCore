package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 20:27
 */
object value02_mapPartitions {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)
    // 3.2 调用mapPartitions方法，每个元素乘以2
//    val value: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, items) => items.map((index, _)))

    val value: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))

    value.foreach(println)
    // 4. 关闭sc
    sc.stop()
  }
}
