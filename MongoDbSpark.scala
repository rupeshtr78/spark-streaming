package com.rupesh

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object MongoDbSpark {

  val spark = SparkSession.builder()
    .appName("Spark MOngoDb")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  import spark.implicits._

  val driver = "org.postgresql.Driver"
  val uri = "mongodb://192.168.1.200:27017/carsdataset.cars"

  def writetoMongoDb() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch{(batch : Dataset[Car] ,batchId: Long) =>
        batch.write
          .format("com.mongodb.spark.sql.DefaultSource")
          .option("uri",uri)
          .mode("append")
          .save()
      }

      .start()
      .awaitTermination()

  }

  import spark.sqlContext.sql

  def readMongodb(): Unit ={
    spark.read
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("uri",uri)
      .load()
      .createOrReplaceTempView("carsview")

    sql(
      """
        |select count(*), Origin from carsview
        |where Horsepower >=100
        |group by Origin
        |""".stripMargin)
      .show()

  }

  def main(args: Array[String]): Unit = {
//    writetoMongoDb()
    readMongodb()
  }
}

