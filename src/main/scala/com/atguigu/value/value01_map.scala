package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 20:21
 */
object value01_map {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3. 具体业务逻辑
    // 3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    // 3.2 调用map方法,每个元素乘以2
    val mapRdd: RDD[Int] = rdd.map(_ * 2)

    // 3.3 打印修改后的RDD中数据
    mapRdd.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
