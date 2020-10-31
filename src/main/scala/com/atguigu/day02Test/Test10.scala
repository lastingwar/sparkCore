package com.atguigu.day02Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 9:04
 */
object Test10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9,10))
    val value: RDD[Int] = rdd.sample(withReplacement = false, 0.5)//不放回
    val value1: RDD[Int] = rdd.sample(withReplacement = true, 2) // 放回
    value.collect()foreach(println)
    value1.collect()foreach(println)
    // 4. 关闭sc
    sc.stop()
  }
}
