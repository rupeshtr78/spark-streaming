package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamRead {

  val spark = SparkSession.builder()
    .appName("DSteam Read")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

  def readDtream() ={
    val dStreamSocket = ssc.socketTextStream("192.168.1.200",1978)

    val wordsStream = dStreamSocket.flatMap(_.split("\n"))

    def friendsForEach() = wordsStream.foreachRDD{ rdd =>
      val friendsDS = spark.createDataset(rdd)
    }


    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readDtream()
  }

}


