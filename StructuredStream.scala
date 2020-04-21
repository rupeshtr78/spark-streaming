package com.rupesh

import common.stocksSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object StructuredStream {

  val spark = SparkSession.builder()
    .appName("Structured Stream")
    .master("local")
    .config("spark.testing.memory",471859200)
    .getOrCreate()

  import spark.implicits._

  def readLinesNetcat = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.1.200")
      .option("port", 1978)
      .load()

    val words = lines.map(x => x.toString().split(" "))

    val query = words.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()

  }

    def readTextFromFile = {
      val stocksDF = spark.readStream
        .format("csv")
        .option("header",false)
        .option("dateFormat","MMM dd yyyy")
        .schema(stocksSchema)
        .load("src/main/resources/data/stocks")

      println(stocksDF.isStreaming)

      stocksDF.writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()

    }

  def simpletrigger ={
    val lines = spark.readStream
      .format("socket")
      .option("host","192.168.1.200")
      .option("port","1978")
      .load()

    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(5.seconds)
//        Trigger.Continuous(2.seconds)
      )
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    println("==========Start Streaming============")

    simpletrigger
  }

}
