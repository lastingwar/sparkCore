package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 18:59
 */
object partition03_file_default {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    //1）默认分区的数量：默认取值为当前核数和2的最小值
    val rdd: RDD[String] = sc.textFile("input")

    rdd.saveAsTextFile("output")


    // 4. 关闭sc
    sc.stop()
  }
}
