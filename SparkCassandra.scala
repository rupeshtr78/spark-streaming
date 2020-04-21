package com.rupesh

import common._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

//Spark connector
import com.datastax.spark.connector._


object SparkCassandra {

  val spark = SparkSession.builder()
    .appName("Spark Cassandra")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .config("spark.cassandra.connection.host","192.168.1.200")
    .config("spark.cassandra.connection.port","9042")
    .config("spark.cassandra.auth.username","cassandra")
    .config("spark.cassandra.auth.password","cassandra")
    .getOrCreate()

  import spark.implicits._

  def writetoCassandra()={
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch{(batch: Dataset[Car] , batchId: Long) =>
        batch.select(col("Name"),col("Horsepower"))
          .write
          .cassandraFormat("cars","hyper")
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()

  }


  import spark.sqlContext.sql

  case class CarClass(Name:String,
                      Horsepower:Int)

  val carSchemaMin = StructType(Array(
    StructField("Name",StringType),
    StructField("Horsepower",IntegerType)
  ))


  import com.datastax.spark.connector.cql.CassandraConnectorConf
  spark.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("192.168.1.200") ++ CassandraConnectorConf.ConnectionPortParam.option(9042))

  def readCassandra(): Unit ={

    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "cars", "keyspace" -> "hyper" , "cluster" -> "Cluster1"))
      .load()
      .createOrReplaceTempView("carsview")


    sql(
      """
        |select * from carsview limit 10
        |""".stripMargin).show()

  }

  val sc = spark.sparkContext

  def readRddCassandara(): Unit ={
   val rdd = sc.cassandraTable("hyper","cars")
    rdd.select("Name","Horsepower").take(5).foreach(println)

  }



  def main(args: Array[String]): Unit = {
//  writetoCassandra()
//    readCassandra()
    readRddCassandara()
  }


}
