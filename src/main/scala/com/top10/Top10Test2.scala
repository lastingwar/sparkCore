package com.top10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable.ListBuffer

/**
 * @author yhm
 * @create 2020-09-27 11:54
 */
object Top10Test2 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val tuples = rdd
//      .map {
//      line =>
//        val strings: Array[String] = line.split("_")
//        strings(6) + "_" + strings(8) + "_" + strings(10)
//    }
      .flatMap(line => {
        val strings: Array[String] = line.split("_")
        List(("click", strings(6)), ("order", strings(8)), ("pay", strings(10)))
      }
      )
      .flatMap {
        case (key, value) =>
          val list: ListBuffer[(String, String)] = ListBuffer()
          if (value.contains(",")) {
            val strings: Array[String] = value.split(",")
            for (i <- strings) {
              list.append((key, i))
            }
          } else {
            if (value != "-1" && value != "null")
              list.append((key, value))
          }
          list
      }.map {
      case (key, value) =>
        (key + "_" + value, 1)
    }.reduceByKey(_ + _)
      .map {
        case (key, value) =>
          val strings: Array[String] = key.split("_")
          (strings(1), (strings(0), value))
      }.groupByKey()
      .mapValues(
        line => {
          val map: Map[String, Int] = line.toList.toMap
          (map.getOrElse("click",0),map.getOrElse("order",0),map.getOrElse("pay",0))
        }
      ).sortBy(_._2,ascending = false)
      .take(10)



    tuples.foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
