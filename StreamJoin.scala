package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StreamJoin {

  val spark = SparkSession.builder()
    .appName("Spark Join")
    .master("local")
    .config("spark.testing.memory",471859200)
    .getOrCreate()

  val friendSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("age",IntegerType),
    StructField("numFriends",IntegerType)

  ))


  val friendIdSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name",StringType),
     ))


  def friendsJoin = {

    val friendsDF = spark.readStream
      .format("socket")
      .option("host","192.168.1.200")
      .option("port","1978")
      .load()
      .select(from_csv(col("value"),friendSchema,Map("sep"-> ",")).as("friends"))
      .selectExpr("friends.id as id","friends.age as age","friends.numFriends as numFriends")

    val friendsIdDF = spark.readStream
      .format("socket")
      .option("host","192.168.1.200")
      .option("port","1987")
      .load()
      .select(from_csv(col("value"),friendIdSchema,Map("sep"->",")).as("friendsId"))
      .selectExpr("friendsId.id as id","friendsId.name as name")

    val friendsJoinDF = friendsIdDF.join(friendsDF,friendsIdDF.col("id") === friendsDF.col("id"))

    friendsJoinDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    friendsJoin
  }

}
