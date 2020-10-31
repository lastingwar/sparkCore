package com.atguigu.day02Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 8:51
 */
object Test06 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5), 5)

    val value: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, list) => list.map((index, _)))
    value.collect().foreach(println)
    sc.stop()
  }
}
