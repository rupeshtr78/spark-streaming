package com.rupesh

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StreamDataset {

  val spark = SparkSession.builder()
    .appName("Stream DataSet")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))


  case class CarsClass(
                      Name:String,
                      Miles_Per_Gallon:Option[Double],
                      Cylinders: Option[Long],
                      Displacement: Option[Double],
                      Horsepower: Option[Long],
                      Weight_in_lbs: Option[Long],
                      Acceleration: Option[Double],
                      Year: String,
                      Origin: String
  )

  import spark.implicits._

  def readDataSetStream() = {
//    val CarEndcoders = Encoders.product[CarsClass]
    spark.readStream
      .format("socket")
      .option("host", "192.168.1.200")
      .option("port", "1978")
      .load()
      .select(from_json(col("value"), carsSchema).as("cars"))
      .selectExpr("cars.*")
//      .as[CarsClass](CarEndcoders)
      .as[CarsClass]
  }


    def showCarNames() = {

      val carsDS:Dataset[CarsClass] = readDataSetStream()

      val carNamesDF:DataFrame = carsDS.select(col("Name"))

      val CarNamesDS = carsDS.map(_.Name)

      CarNamesDS.writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()
  }



  def main(args: Array[String]): Unit = {
    showCarNames()
  }

}
