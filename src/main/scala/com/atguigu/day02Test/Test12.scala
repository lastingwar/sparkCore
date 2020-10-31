package com.atguigu.day02Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 9:11
 */
object Test12 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/1.txt")
//    val value: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//    value.collect().foreach(println)
    val value1: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1))

    value1.groupBy(_._1).map(list=>(list._1,list._2.size))
    .collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
