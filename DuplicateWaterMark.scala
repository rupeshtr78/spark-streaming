package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object DuplicateWaterMark extends App {

  val spark = SparkSession.builder()
    .appName("DeDuplication WaterMark")
    .master("local")
    .config("spark.testing.memory", 2147480000) //471859200
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()


  import spark.implicits._

//  spark.sparkContext.setLogLevel("ERROR")

  case class WaterMark(time: Int, id:Int)

  val socketStreamDS = spark.readStream
    .format("socket")
    .option("host", "192.168.1.200")
    .option("port", 50050)
    .load()
    .as[String]
    .map{line =>
      val tokens = line.split(",")
      WaterMark(tokens(0).toInt,tokens(1).toInt)}
    .toDF()

  val duplicateWaterDS = socketStreamDS
    .withColumn("time", $"time" cast ("timestamp"))
    .withWatermark("time", "1 second")
    .groupBy(window(col("time") ,"2 second").as("wTime"))
    .agg(sum(col("id")).as("count"))
//    .dropDuplicates("id")
    .select(
      col("wTime").getField("start").cast("long").as("start"),
      col("wTime").getField("end").cast("long").as("end"),
      col("count")
      )
//    .withColumn("time", $"time" cast ("long"))

  val q = duplicateWaterDS.
    writeStream.
    format("console").
//    queryName("dups").
    outputMode(OutputMode.Append()).
//    trigger(Trigger.ProcessingTime(1.seconds)).
    option("checkpointLocation", "checkpoint-dir"). // <-- use checkpointing to save state between restarts
    start().
    awaitTermination()



}
