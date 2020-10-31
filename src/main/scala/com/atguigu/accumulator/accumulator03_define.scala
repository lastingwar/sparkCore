package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext, rdd}

import scala.collection.mutable

/**
 * @author yhm
 * @create 2020-09-27 11:30
 */
object accumulator03_define {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val acc = new MyAcc()
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"), 2)

    sc.register(acc)

    rdd.foreach(
      word => {
        acc.add(word)
      }
    )

    println(acc.value)

    // 4. 关闭sc
    sc.stop()
  }
}

// 声明累加器
// 1.继承AccumulatorV2,设定输入、输出泛型
// 2.重新方法
class MyAcc extends AccumulatorV2[String, mutable.Map[String, Long]] {
  // 定义输出数据集合
  var map = mutable.Map[String, Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAcc

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    // 业务逻辑
    if (v.startsWith("H")) {
      map(v) = map.getOrElse(v, 0L) + 1L
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    other.value.foreach {
      case (word, count) => {
        map(word) = map.getOrElse(word, 0L) + count
      }
    }
  }

  override def value: mutable.Map[String, Long] = map
}