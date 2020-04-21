package com.rupesh

import common.carsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafka {

  val spark = SparkSession.builder()
    .appName("Spark Kafka")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

  def readFromKafka() = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.181:9092")
//      .option("subscribe","streams-spark-input")
      .option("assign", "{\"streams-spark-input3\":[0]}")
      .load()


    kafkaDF
      .select(col("topic") , col("value").cast(StringType))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def writeKafka() = {
    val carsDF = spark.readStream
        .schema(carsSchema)
        .json("src/main/resources/data/cars")

    val carsWriteKafka = carsDF.selectExpr("upper(Name) as key","Name as value")

    val carsWriteKafkaFull = carsDF.select(
      col("name").as("key"),
//      to_json(struct(col("Horsepower"),col("Origin") )).as("value"))
      to_json(struct(col("*"))).as("value"))



    carsWriteKafkaFull.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers","192.168.1.200:9092")
        .option("topic","sparkstreaming")
        .option("checkpointLocation","checkpoint")
        .start()
        .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromKafka()

  }

}

