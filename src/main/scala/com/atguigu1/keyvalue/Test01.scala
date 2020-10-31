package com.atguigu1.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * @author yhm
 * @create 2020-09-24 19:29
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 2), (2, 3), (4, 2), (5, 1), (1, 9)), 2)

    // partitionBy
    rdd.partitionBy(new HashPartitioner(2)).collect().foreach(println)

    // 自定义分区
    rdd.partitionBy(new Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = {
        key match {
          case i: Int =>
            i % 2
          case _ =>
            0
        }
      }
    }).collect().foreach(println)

    // reduceByKey
    rdd.reduceByKey(_ + _).collect().foreach(println)

    // groupByKey
    rdd.groupByKey().collect().foreach(println)

    // aggregateByKey
    rdd.aggregateByKey(0)(math.max, _ + _).collect().foreach(println)

    // foldByKey
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    // combineByKey
    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)
    input.combineByKey(
      (_, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    ).collect().foreach(println)

    // sortByKey
    rdd.sortByKey(ascending = true).collect().foreach(println)

    // mapValues
    rdd.mapValues(_ + 1).collect().foreach(println)

    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 2), (3, 4)))
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((4, 6), (7, 8)))

    // join
    rdd1.join(rdd2).collect().foreach(println)

    // cogroup
    rdd1.cogroup(rdd2).collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
