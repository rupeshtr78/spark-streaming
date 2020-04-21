package com.rupesh

import java.net.Socket

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.io.Source

class CustomSocketReceiver(host: String , port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  val socketPromise = Promise[Socket]()
  val socketFuture = socketPromise.future

  override def onStart(): Unit = {

    val socket = new Socket(host,port)

    Future{
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line))

    }

    socketPromise.success(socket)
  }

  override def onStop(): Unit = socketFuture.foreach(socket=> socket.close())

}

object CustomReceiver {

  val spark = SparkSession.builder()
    .appName("Custom Receiver")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))



  def main(args: Array[String]): Unit = {
   val dataStream = ssc.receiverStream(new CustomSocketReceiver("192.168.1.200",1978))
    dataStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
