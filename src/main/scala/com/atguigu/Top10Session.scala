package com.atguigu

import com.top10.{CategoryCountInfo, MyAcc1, UserVisitAction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author yhm
 * @create 2020-09-28 10:16
 */
object Top10Session {
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

    val top10: List[CategoryCountInfo] = value.groupBy(_._1._1).map {
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

    val list: List[String] = top10.map(_.categoryId)

    val bro: Broadcast[List[String]] = sc.broadcast(list)


    // 4. 关闭sc
    sc.stop()
  }
}
