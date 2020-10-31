package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 18:54
 */
object partition02_Array {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    //1）4个数据，设置4个分区，输出：0分区->1，1分区->2，2分区->3，3分区->4
    //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)

    //2）4个数据，设置3个分区，输出：0分区->1，1分区->2，2分区->3,4
    //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 3)

    //3）5个数据，设置3个分区，输出：0分区->1，1分区->2、3，2分区->4、5
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5), 3)

    rdd.saveAsTextFile("output")


    // 4. 关闭sc
    sc.stop()
  }
}
