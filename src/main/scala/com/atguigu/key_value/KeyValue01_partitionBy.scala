package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 14:13
 */
object KeyValue01_partitionBy {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3 具体业务逻辑
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    //    rdd.partitionBy(new HashPartitioner(2)).collect().foreach(println)

    rdd.partitionBy(new Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = {
        key match {
          case i: Int =>
            i % 2
          case _ => 0
        }
      }
    }).collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
