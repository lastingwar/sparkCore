package com.atguigu.dempTop3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-24 18:06
 */
object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    //2. 读取日志文件，获取原始数据
    val rdd: RDD[String] = sc.textFile("input/agent.log")
    //3. 将原始数据进行结构转换string =>(prv-adv,1)
    val rdd1: RDD[(String, Int)] = rdd.map(
      lines => {
        val strings: Array[String] = lines.split(" ")
        (strings(1) + "-" + strings(4), 1)
      }
    )

    //4. 将转换结构后的数据进行聚合统计（prv-adv,1）=>(prv-adv,sum)
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    //5. 将统计的结果进行结构的转换（prv-adv,sum）=>(prv,(adv,sum))
    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case (key, value) => {
        val strings: Array[String] = key.split("-")
        (strings(0), (strings(1), value))
      }
    }
    //6. 根据省份对数据进行分组：(prv,(adv,sum)) => (prv, Iterator[(adv,sum)])
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()
    //7. 对相同省份中的广告进行排序（降序），取前三名
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues(value => {
      value.toList.sortWith {
        case (lift, right) => {
          lift._2 > right._2
        }
      }.take(3)
    })
    //8. 将结果打印
      rdd5.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
