package com.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-25 8:43
 */
object day03Test02 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("c", 1), ("a", 1), ("a", 1)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("c", 1), ("a", 1), ("a", 1)))

    // 1. reduceByKey 按照key聚合
    rdd1.reduceByKey(_+_).collect().foreach(println)

    // 2. groupByKey 按照key分组
    rdd1.groupByKey().collect().foreach(println)

    // 3. foldByKey 分区内和分区间方法相同
    rdd1.foldByKey(0)(_+_).collect().foreach(println)

    // 4. aggregateByKey 选取各分区key对应value的最大值,最后相加
    rdd1.aggregateByKey(0)(math.max,_+_).collect().foreach(println)

    // 5. combineByKey 聚合求平均值
    rdd1.combineByKey((_,1),
      (key:(Int,Int),v)=>(key._1+v,key._2+1),
      (key:(Int,Int),v:(Int,Int))=>{
        (key._1+v._1,key._2+v._2)
      }
    ).collect().foreach(println)

    // 6. partitionBy 使用分区器分区
    rdd1.partitionBy(new HashPartitioner(2)).collect().foreach(println)

    // 7. join
    rdd1.join(rdd2).collect().foreach(println)

    // 8. sortByKey 排序
    rdd1.sortByKey().collect().foreach(println)

    // 9. mapValues
    rdd1.mapValues(_+1).collect().foreach(println)

    // 10. 全外连接
    rdd1.cogroup(rdd2).collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
