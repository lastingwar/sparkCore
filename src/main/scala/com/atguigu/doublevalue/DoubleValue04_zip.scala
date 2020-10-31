package com.atguigu.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 14:10
 */
object DoubleValue04_zip {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3),3)

    //3.2 创建第二个RDD
    val rdd2: RDD[String] = sc.makeRDD(Array("a","b","c"),3)

    rdd1.zip(rdd2).collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
