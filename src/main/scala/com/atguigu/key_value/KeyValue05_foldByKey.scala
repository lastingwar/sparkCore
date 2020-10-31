package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 14:39
 */
object KeyValue05_foldByKey {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
    val list: List[(String, Int)] = List(("a", 1), ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1), ("a", 1))

    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    rdd.aggregateByKey(0)(math.max,_+_).collect().foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}
