package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-22 20:41
 */
object value04_flatMap {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3.1 创建一个RDD
//    val listRDD=sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7)), 2)
//
//    // 3.2 把所有子集合中数据取出放入到一个大的集合中
//    val value: RDD[Int] = listRDD.flatMap(list => {
//      val list1: List[Int] = list
//      list1
//    })
//    val value1: RDD[Int] = value.map(_ + 1)

    val listRDD=sc.makeRDD(List(List("1","2"),List("2","3"),List("3","4"),List("2")), 2)

    // 3.2 把所有子集合中数据取出放入到一个大的集合中
    val value: RDD[String] = listRDD.flatMap(list => list)
    val value1: RDD[Int] = value.map(_.toInt + 1)
    value1.collect().foreach(println)
    // 4. 关闭sc
    sc.stop()
  }
}
