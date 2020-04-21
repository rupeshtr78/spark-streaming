package com.rupesh

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object PostGresReadWrite {

  val spark = SparkSession.builder()
    .appName("Spark Postgres")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  import spark.implicits._

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://192.168.1.200:5432/hyper"
  val user = "postgres"
  val password = "postgres"

  def writetoJdbc() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch{(batch : Dataset[Car] ,batchId: Long) =>
        batch.write
          .format("jdbc")
          .option("driver",driver)
          .option("url",url)
          .option("user",user)
          .option("password",password)
          .option("dbtable","public.cars")
          .save()
      }

      .start()
      .awaitTermination()

  }

  import spark.sqlContext.sql

  def readPostGres(): Unit ={
    spark.read
      .format("jdbc")
      .option("driver",driver)
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","public.cars")
      .load()
      .createOrReplaceTempView("carsview")

    sql(
      """
        |select * from carsview limit 10
        |""".stripMargin)
      .show()

  }

  def main(args: Array[String]): Unit = {
//    writetoJdbc()
    readPostGres()
  }
}
