package com.top10test1

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author yhm
 * @create 2020-09-27 19:33
 */
object Top10Test5 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val actionRDD: RDD[UserVisitAction] = rdd.map(line => {
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
    })
    val acc = new MyAcc()
    sc.register(acc,"acc")

    actionRDD.foreach(data=>acc.add(data))

    // (id,click)->num
    val value: mutable.Map[(String, String), Long] = acc.value

    value.groupBy(_._1._1).map{
      case (key,value)=>
        val clickNum: Long = value.getOrElse((key, "click"), 0L)
        val orderNum: Long = value.getOrElse((key, "order"), 0L)
        val payNum: Long = value.getOrElse((key, "pay"), 0L)
        CategoryCountInfo(key,clickNum,orderNum,payNum)
    }.toList.sortWith((left,right)=>{
      if (left.clickCount > right.clickCount) {
        true
      } else if (left.clickCount == right.clickCount){
        if (left.clickCount > right.clickCount)
          true
        else left.payCount > right.payCount
      }else false
    }).take(10).foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}


class MyAcc extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{

  val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new MyAcc

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    if (v.click_category_id != -1){
      val key: (String, String) = (v.click_category_id.toString, "click")
      map(key) = map.getOrElse(key,0L) + 1L
    }else if (v.order_category_ids != "null"){
      val strings: Array[String] = v.order_category_ids.split(",")
      for (elem <- strings) {
        val key: (String, String) = (elem, "order")
        map(key) = map.getOrElse(key,0L) + 1L
      }
    }else if (v.pay_category_ids != "null"){
      val strings: Array[String] = v.pay_category_ids.split(",")
      for (elem <- strings) {
        val key: (String, String) = (elem, "pay")
        map(key) = map.getOrElse(key,0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach{
      case (key,value)=>
        map(key) = map.getOrElse(key,0L) + value
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}