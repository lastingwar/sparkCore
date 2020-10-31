package com.top10

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author yhm
 * @create 2020-09-27 20:24
 */
object Top10session10 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val acc: MyAcc1 = new MyAcc1()

    sc.register(acc, "acc")

    val actionRdd: RDD[UserVisitAction] = rdd.map(line => {
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

    actionRdd.foreach(
      data => {
        acc.add(data)
      }
    )

    val value: mutable.Map[(String, String), Long] = acc.value

    val infoes: List[CategoryCountInfo] = value.groupBy(_._1._1).map {
      case (id, map) =>
        val click: Long = map.getOrElse((id, "click"), 0L)
        val order: Long = map.getOrElse((id, "order"), 0L)
        val pay: Long = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
    }.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.payCount == right.payCount) {
            left.payCount > right.payCount
          } else false
        } else false
      }
    }.take(10)

    val longs: ListBuffer[Long] = ListBuffer[Long]()
    for (elem <- infoes) {
      longs.append(elem.categoryId.toLong)
    }

    val broadcast: Broadcast[ListBuffer[Long]] = sc.broadcast(longs)

    val value1: RDD[UserVisitAction] = actionRdd.filter(elem => {
      if (elem.click_category_id != -1) {
        broadcast.value.contains(elem.click_category_id)
      } else false
    }
    )
    value1.map(action => (action.click_category_id + "--" + action.session_id, 1)).reduceByKey(_ + _)
      .map {
        case (key, value) => {
          val strings: Array[String] = key.split("--")
          (strings(0), (strings(1), value))
        }
      }.groupByKey()
      .mapValues(dates => dates.toList.sortWith(
      (left, right) => left._2 > right._2
    ).take(10)).foreach(println)
    // 4. 关闭sc
    sc.stop()
  }
}

class MyAcc1 extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {

  val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new MyAcc1()

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    if (v.click_category_id != -1) {
      val clickID: (String, String) = (v.click_category_id.toString, "click")

      map(clickID) = map.getOrElse(clickID, 0L) + 1L
    }
    else if (v.order_category_ids != "null") {
      val strings: Array[String] = v.order_category_ids.split(",")
      for (elem <- strings) {
        val orderID: (String, String) = (elem, "order")
        map(orderID) = map.getOrElse(orderID, 0L) + 1L
      }
    } else if (v.pay_category_ids != "null") {
      val strings: Array[String] = v.pay_category_ids.split(",")
      for (elem <- strings) {
        val payID: (String, String) = (elem, "pay")
        map(payID) = map.getOrElse(payID, 0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {

    other.value.foreach {
      case (cate, count) => {
        map(cate) = map.getOrElse(cate, 0L) + count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}