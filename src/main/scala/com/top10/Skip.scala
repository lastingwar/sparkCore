package com.top10

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-28 11:14
 */
object Skip {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

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
    val bro: Broadcast[List[Long]] = sc.broadcast(List(1, 2, 3, 4, 5, 6, 7))
    val idZipList: List[String] = bro.value.zip(bro.value.tail).map {
      case (page1, page2) => {
        page1 + "-" + page2
      }
    }

    val pageFlowRDD: RDD[List[String]] = actionRdd.groupBy(_.session_id)
      .mapValues(
        dates => {
          // 5.2.1 对分组后的数据进行排序
          val actions: List[UserVisitAction] = dates.toList.sortWith(
            (left, right) => {
              left.action_time < right.action_time
            }
          )
          // 5.2.2 获取PageId
          val pageIdList: List[Long] = actions.map(_.page_id)
          // 5.2.3 形成单跳元祖
          val pageToPageList: List[(Long, Long)] = pageIdList.zip(pageIdList.tail)
          val pageJumpCounts: List[String] = pageToPageList.map {
            case (page1, page2) =>
              page1 + "-" + page2
          }
          // 5.2.5 再次进行过滤,减轻计算负担
          pageJumpCounts.filter(data => idZipList.contains(data))
        }
      ).map(_._2)
    val fenzi: RDD[(String, Long)] = pageFlowRDD.flatMap(list => list).map((_, 1L)).reduceByKey(_ + _)


    val fenmu: Map[Long, Long] = actionRdd.filter(data => bro.value.init.contains(data.page_id))
      .map(data => (data.page_id, 1L))
      .reduceByKey(_ + _).collect().toMap
    fenmu.foreach(println)
    fenzi.foreach(println)
    fenzi.foreach {
      case (pageflow, sum) =>
        val strings: Array[String] = pageflow.split("-")
        val pageIdSum: Long = fenmu.getOrElse(strings(0).toLong, 1L)
        println(pageflow + "=" + sum.toDouble / pageIdSum)
    }

    // 4. 关闭sc
    sc.stop()
  }
}
