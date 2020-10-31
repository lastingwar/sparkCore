package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 18:30
 */
object createrdd02_file {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3. 读取文件.如果是集群路径: hdfs://hadoop102:8020/input
    val lineWordRdd: RDD[String] = sc.textFile("input")

    // 4. 打印
    lineWordRdd.foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
