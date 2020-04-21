package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object FriendsBasics extends App {

  val spark = SparkSession.builder()
    .appName("Friends")
    .master("local")
    .config("spark.testing.memory",471859200)
    .getOrCreate()

  val userSchema = new StructType()
    .add("id", "integer")
    .add("name", "string")
    .add("age", "integer")
    .add("friends", "integer")


  val friendsDF = spark.read
    .option("inferSchema","true")
    .schema(userSchema)
    .csv("src/main/resources/data/fakefriends.csv")

//  friendsDF.show()

  friendsDF
    .groupBy(col("age"))
    .agg(
      sum("friends").as("num_Friends")
    )
    .orderBy(col("num_Friends").desc_nulls_last).show()

}
