package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//Spark connector
import com.datastax.spark.connector.cql.CassandraConnectorConf


object ReadCassandra {

  val spark = SparkSession.builder()
    .appName("Spark Cassandra")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  spark.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("192.168.1.200") ++ CassandraConnectorConf.ConnectionPortParam.option(9042))

  val sc = spark.sqlContext

  case class CarClass(Name:String,
                      Horsepower:Int)

  val carSchemaMin = StructType(Array(
    StructField("Name",StringType , false),
    StructField("Horsepower",IntegerType, true)
  ))


  def readCassandra(): Unit = {

   sc.read
     .format("org.apache.spark.sql.cassandra")
     .options(Map("table" -> "cars", "keyspace" -> "hyper","Cluster"->"Cluster1"))
//     .schema(carSchemaMin)
     .load()
      .show()
//      .printSchema()
//     .createOrReplaceTempView("carsview")
//
//   sc.sql(
//     """
//       |select * from carsview limit 10
//       |""".stripMargin).show()

 }

  def main(args: Array[String]): Unit = {
    readCassandra()
  }


}
