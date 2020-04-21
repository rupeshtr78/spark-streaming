package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StreamWindow {

  val spark = SparkSession.builder()
    .appName("Spark Window")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val windowSchema = StructType(Array(
    StructField("UserId", StringType),
    StructField("UUID",StringType),
//    StructField("timestamp",TimestampType),
    StructField("timestamp",StringType),
    StructField("qty",IntegerType)
  ))

  def readWindow() = spark.readStream
    .format("socket")
    .option("host","192.168.1.200")
    .option("port","1978")
    .load()
    .select(from_csv(col("value"),windowSchema,Map("sep"->",")).as("windowStream"))
//    .selectExpr("windowStream.timestamp" , "windowStream.qty")
    .select(
      col("windowStream").getField("timestamp").cast("timestamp").as("timestamp"),
      col("windowStream").getField("qty").as("qty")
          )


  def aggBySlidingWindow() = {
    val windowDF = readWindow()
    windowDF.printSchema()

    val windowbyMinutes = windowDF
      .groupBy(window(col("timestamp"),"20 minute","10 minute").as("time"))
      .agg(sum(col("qty")).as("totalQty"))
        .select(
        col("time").getField("start").as("start")
        ,col("time").getField("end").as("end")
        ,col("totalQty"))
        .orderBy(col("start").desc)

    windowbyMinutes.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggBySlidingWindow()
  }

}
