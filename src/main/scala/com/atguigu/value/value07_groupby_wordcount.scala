package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-23 9:46
 */
object value07_groupby_wordcount {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")
    val rdd = sc.makeRDD(strList)

    val value: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1))

    value.groupBy(_._1).map{
      case (word,list)=>(word,list.size)
    }.collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
