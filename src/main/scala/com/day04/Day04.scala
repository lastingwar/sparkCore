package com.day04

import com.top10test1.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author yhm
 * @create 2020-09-28 8:27
 */
object Day04 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")


    val actRdd: RDD[UserVisitAction] = rdd.map(line => {
      val strings: Array[String] = line.split("_")
      UserVisitAction(
        strings(0),
        strings(1).toLong,
        strings(2),
        strings(3).toLong,
        strings(4),
        strings(5),
        strings(6).toLong,
        strings(7).toLong,
        strings(8),
        strings(9),
        strings(10),
        strings(11),
        strings(12).toLong
      )
    }
    )
    actRdd.flatMap(dates=>{
      if (dates.click_category_id != -1){
        val key: String = dates.click_category_id.toString
        List((key,CategoryCountInfo(key,1,0,0)))
      }else if (dates.order_category_ids != "null"){
        val strings: Array[String] = dates.order_category_ids.split(",")
        val list: ListBuffer[(String, CategoryCountInfo)] = ListBuffer[(String, CategoryCountInfo)]()
        for (elem <- strings) {
          list.append((elem,CategoryCountInfo(elem,0,1,0)))
        }
        list
      }else if (dates.pay_category_ids != "null"){
        val strings: Array[String] = dates.pay_category_ids.split(",")
        val list: ListBuffer[(String, CategoryCountInfo)] = ListBuffer[(String, CategoryCountInfo)]()
        for (elem <- strings) {
          list.append((elem,CategoryCountInfo(elem,0,0,1)))
        }
        list
      }else Nil
    }
    ).reduceByKey((left,right)=>{
      left.clickCount += right.clickCount
      left.orderCount += right.orderCount
      left.payCount += right.payCount
      left
    }).map(_._2).sortBy(data=>{
      (data.clickCount,data.orderCount,data.payCount)
    }).take(10).foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}

