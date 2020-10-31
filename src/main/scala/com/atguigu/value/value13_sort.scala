package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 13:03
 */
object value13_sort {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3. 具体业务逻辑
    // 3.1 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(2, 3, 1, 4, 6, 5))
    val sortRdd: RDD[Int] = rdd.sortBy(num => num)
    sortRdd.collect().foreach(println)

    val sortRdd2: RDD[Int] = rdd.sortBy((num => num), false)
    sortRdd2.collect().foreach(println)

    val strRdd: RDD[String] = sc.makeRDD(List("1", "22", "12", "2", "3"))

    strRdd.sortBy(num=>num.toInt).collect().foreach(println)

    // 3.5 创建一个RDD
    val rdd3: RDD[(Int, Int)] = sc.makeRDD(List((2, 1), (1, 2), (1, 1), (2, 2)))

    rdd3.sortBy(t=>t).collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
