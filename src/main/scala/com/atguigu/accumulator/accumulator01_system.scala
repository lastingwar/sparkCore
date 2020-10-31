package com.atguigu.accumulator

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
 * @author yhm
 * @create 2020-09-27 11:26
 */
object accumulator01_system {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    val acc: LongAccumulator = sc.longAccumulator("acc")

    rdd.foreach{
      case (a,mount)=>{
        acc.add(mount)
      }
    }
    println(acc.value)
    // 4. 关闭sc
    sc.stop()
  }
}
