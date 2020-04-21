package com.rupesh

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RateSource extends App {


  val spark = SparkSession.builder()
    .appName("Stream Agg")
    .master("local")
    //    .config("spark.testing.memory",471859200)
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

   import spark.implicits._

  val rates = spark
    .readStream
    .format("rate") // <-- use RateStreamSource
    .option("rowsPerSecond", "1")
    .option("rampUpTime", "1")
    .load



  val schema = rates.schema
  println(schema.treeString)

   case class RateSource(time:Long , value: Long)

  val input = spark.readStream
    .format("rate")
    .option("rowsPerSecond", "1")
    .option("rampUpTime", "1")
    .load()
    .as[(Timestamp, Long)]
    .map { v =>
      val newTime = v._1.getTime
      RateSource(newTime, v._2)
    }


  val windowEvent = rates
    .groupBy(window(col("timestamp"), "1 minute", "10 second").as("time"))
    .agg(sum(col("value")).as("totalValue"))
    .select(col("time").getField("start").as("Start"),
      col("time").getField("end").as("end"),
      col("totalValue"))

    val watermarkWindow = rates
      .withWatermark(eventTime="timestamp", delayThreshold="10 seconds")
      .groupBy(window(col("timestamp"), "1 minute", "10 second").as("time"))
      .agg(sum(col("value")).as("totalValue"))
      .select(col("time").getField("start").as("Start"),
        col("time").getField("end").as("end"),
        col("totalValue"))

  watermarkWindow
       .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
}
