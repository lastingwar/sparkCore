package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 18:07
 */
object createrdd01_array {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc:SparkContext = new SparkContext(conf)

    // 3.使用 parallelize()创建rdd
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8))

    rdd.collect().foreach(println)

    // 4. 使用makeRDD()创建 rdd
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7))
    rdd1.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()

  }
}
