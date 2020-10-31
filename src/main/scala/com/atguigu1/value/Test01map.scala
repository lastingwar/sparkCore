package com.atguigu1.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 18:28
 */
object Test01map {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    //rdd: 0:(1,2,3)  1:(4,5,6)
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)

    //rdd1: 0:"hello world"  1:"world count"
    val rdd1: RDD[String] = sc.makeRDD(Array("hello world", "word count","hello word count"),2)

    // map映射
    rdd.map(_ + 1).collect().foreach(println)

    // mapPartitions
    rdd.mapPartitions(_.map(_ + 1)).collect().foreach(println)

    // mapPartitionsWithIndex
    rdd.mapPartitionsWithIndex((par,value)=>{
      value.map((par,_))
    }).collect().foreach(println)

    // flatMap
    rdd1.flatMap(_.split(" ")).collect().foreach(println)

    // glom
    rdd.glom().collect().foreach(println)

    // groupBy
    rdd1.flatMap(_.split(" ")).groupBy(key=>key).map{
      case (key,value) =>{
        (key,value.size)
      }
    }.collect().foreach(println)

    // filter
    rdd.filter(_>2).collect().foreach(println)

    // sample
    rdd.sample(withReplacement = false,0.5).collect().foreach(println)

    //distinct
    rdd.distinct().collect().foreach(println)

    //coalesce  默认不进行shuffle
    rdd.coalesce(3).collect().foreach(println)

    //repartition 进行shuffle的coalesce
    rdd.repartition(3)

    // sortBy
    rdd.sortBy(key=>key).collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
