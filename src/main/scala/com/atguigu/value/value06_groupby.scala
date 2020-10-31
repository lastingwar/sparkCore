package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 21:17
 */
object value06_groupby {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

//    val rdd = sc.makeRDD(1 to 4, 2)
//
//    rdd.groupBy(_%2).collect().foreach(println)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "hive", "hadoop", "spark", "scala"))

    rdd.groupBy(str => str.charAt(0)).collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
