package com.sparksteam1

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yhm
 * @create 2020-10-31 14:07
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(3))

    val receiver: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))

    receiver.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .print()

    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()


    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  override def onStart(): Unit = {
    new Thread("receiver"){
      override def run(): Unit = {
        receiver()
      }
    }.start()
  }

  def receiver(): Unit ={
    var socket: Socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
    var line: String = reader.readLine()

    while (line != null && !isStopped()) {
      store(line)
      line = reader.readLine()
    }
    reader.close()
    socket.close()

    restart("re")

  }

  override def onStop(): Unit = {

  }
}