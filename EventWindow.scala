package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EventWindow {


  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def readPurchasesFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    * 1) Show the best selling product of every day, +quantity sold.
    * 2) Show the best selling product of every 24 hours, updated every hour.
    */

    def bestSellerByday() = {
      val bestSellerDF = readPurchasesFromFile()

      val bestSellerByDayDF= bestSellerDF
//        .select(col("time"),col("item"),col("quantity"))
        .groupBy(window(col("time"),"1 day").as("time") , col("item"))
        .agg(sum(col("quantity")).as("qty"))
        .orderBy(col("time"))


      val results = bestSellerByDayDF
        .groupBy(col("time"), col("item"))
        .agg(max(col("qty")))

      results.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()

    }


  def main(args: Array[String]): Unit = {
    bestSellerByday()
  }
}
