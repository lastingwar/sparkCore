package com.top10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer



/**
 * @author yhm
 * @create 2020-09-27 14:53
 */
object Top10Test4 {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    rdd.map(line=> {
      val dates: Array[String] = line.split("_")
      UserVisitAction(
        dates(0),
        dates(1).toLong,
        dates(2),
        dates(3).toLong,
        dates(4),
        dates(5),
        dates(6).toLong,
        dates(7).toLong,
        dates(8),
        dates(9),
        dates(10),
        dates(11),
        dates(12).toLong
      )
    }
    ).flatMap(cate=>
      if (cate.click_category_id != -1){
        List((cate.click_category_id.toString,CategoryCountInfo(cate.click_category_id.toString,1,0,0)))
      }else if (cate.order_category_ids != "null"){
        val list: ListBuffer[(String, CategoryCountInfo)] = ListBuffer[(String,CategoryCountInfo)]()
        val strings: Array[String] = cate.order_category_ids.split(",")
        for (s <- strings){
          list.append((s,CategoryCountInfo(s,0,1,0)))
        }
        list
      }else if (cate.pay_category_ids != "null"){
        val list: ListBuffer[(String, CategoryCountInfo)] = ListBuffer[(String,CategoryCountInfo)]()
        val strings: Array[String] = cate.pay_category_ids.split(",")
        for (s <- strings){
          list.append((s,CategoryCountInfo(s,0,0,1)))
        }
        list
      }else Nil
    ).reduceByKey((lift,right)=>{
      lift.clickCount += right.clickCount
      lift.orderCount += right.orderCount
      lift.payCount +=  right.payCount
      lift
    })
//      .groupBy(_.categoryId)
//      .mapValues(dates=>dates.reduce(
//        (lift,right)=> {
//          lift.clickCount += right.clickCount
//          lift.orderCount += right.orderCount
//          lift.payCount += right.payCount
//          lift
//        }
//      ))
      .map(_._2)
      .sortBy(cate=>(cate.clickCount,cate.orderCount,cate.payCount),false)
      .take(10).foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
