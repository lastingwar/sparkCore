package com

import com.top10.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-28 14:47
 */
object PageFlow {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val actionRdd: RDD[UserVisitAction] = rdd.map(dates => {
      val strings: Array[String] = dates.split("_")
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
    // 求分母
    val ids: List[Long] = List(1, 2, 3, 4, 5, 6, 7)

    val filterRDD: RDD[UserVisitAction] = actionRdd.filter(data => {
        ids.contains(data.page_id)
    })

    val fenmu: Map[Long, Long] = filterRDD.map(data =>
      (data.page_id, 1L)
    ).reduceByKey(_ + _).collect().toMap

    // 求分子
    val idsZip: List[(Long, Long)] = ids.zip(ids.tail)


    val fenzi: Map[String, Long] = actionRdd.groupBy(_.session_id)
      // 提前过滤,按照session分组中至少有2个1-7的pageID吧
      .filter{
        case (sessionID,list)=>{
          var bool = 0
          list.foreach(data=>
            if (ids.contains(data.page_id)){
             bool += 1
            }
          )
          bool >= 2
        }
      }
      .mapValues {
        data => {
          // 对象类型转换为2元组
          val tuples: Iterable[(String, Long)] = data.map(line => (line.action_time, line.page_id))
          // 按照时间排序
          val longs: List[Long] = tuples.toList.sortBy(_._1).map(_._2)
          val tuples1: List[(Long, Long)] = longs.zip(longs.tail)
          // 过滤
          val strings: List[String] = tuples1.filter(data => idsZip.contains(data))
            // 调整结构
            .map {
              case (page1, page2) =>
                page1 + "-" + page2
            }
          strings
        }
      }.map(_._2).flatMap(list => list).map(data => (data, 1L)).reduceByKey(_ + _).collect().toMap


    fenmu.foreach(println)
    fenzi.foreach(println)
    // 求概率
    fenzi.foreach{
      case (key,value)=>{
        val strings: Array[String] = key.split("-")
        val l: Long = fenmu.getOrElse(strings(0).toLong, 0)
        println( key + "=" + (value.toDouble / l ))
      }
    }





    // 4. 关闭sc
    sc.stop()
  }
}
