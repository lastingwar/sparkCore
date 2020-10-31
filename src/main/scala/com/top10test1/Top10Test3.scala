package com.top10test1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer


/**
 * @author yhm
 * @create 2020-09-27 18:22
 */
object Top10Test3 {
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

    //(id,clickNum,orderNum,payNum)
    actionRdd.flatMap(cate=>{
      if (cate.click_category_id != -1){
        List(CategoryCountInfo(cate.click_category_id.toString,1,0,0))
      }else if(cate.order_category_ids != "null"){
        val strings: Array[String] = cate.order_category_ids.split(",")
        val tuples: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
        for (elem <- strings) {
          tuples.append(CategoryCountInfo(elem,0,1,0))
        }
        tuples
      }else if (cate.pay_category_ids != "null"){
        val strings: Array[String] = cate.pay_category_ids.split(",")
        val tuples: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
        for (elem <- strings) {
          tuples.append(CategoryCountInfo(elem,0,0,1))
        }
        tuples
      }else Nil
    })
      .groupBy(cate=>cate.categoryId)
      .mapValues(
        dates => dates.reduce(
          (left,right)=>{
            left.clickCount += right.clickCount
            left.orderCount += right.orderCount
            left.payCount += right.payCount
            left
          }
        )
      ).map{
      case(key,value)=>{
        value
      }
    }.sortBy(data =>(data.clickCount,data.orderCount,data.payCount),ascending = false)
      .take(10).foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}
