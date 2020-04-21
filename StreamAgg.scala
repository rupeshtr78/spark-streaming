package com.rupesh

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StreamAgg {

  val spark = SparkSession.builder()
    .appName("Stream Agg")
    .master("local")
//    .config("spark.testing.memory",471859200)
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val friendsSchema = StructType(Array(
    StructField("id", IntegerType ),
    StructField("name",StringType),
    StructField("age",IntegerType),
    StructField("friends",IntegerType)
  )
  )

  val userSchema = new StructType()
    .add("id", "integer")
    .add("name", "string")
    .add("age", "integer")
    .add("friends", "integer")



  def friendsCount ={
    val lines = spark.readStream
      .format("socket")
      .option("host","192.168.1.200")
      .option("port","1978")
      .load()

    val friendsDF = lines.select(
      split(col("value"),",")(0).cast("integer").as("id"),
      split(col("value"),",")(1).as("name"),
      split(col("value"),",")(2).cast("integer").as("age"),
      split(col("value"),",")(3).cast("integer").as("numFriends")
    )

    val friendsComposit = lines.select(from_csv(col("value"), friendsSchema, Map("sep" -> ",")).as("friends"))
    friendsComposit.printSchema()

    val friendsCompDF = friendsComposit.selectExpr("friends.age as age","friends.friends as numFriends")


    val friendsByAge = friendsCompDF
      .groupBy(col("age"))
      .agg(
        sum("numFriends").as("num_Friends")
      )

    val nameCount = friendsDF
      .select(col("name"))
      .groupBy(col("name"))
      .count()



    friendsCompDF.writeStream
      .format("console")
//      .start()
//      .awaitTermination()


        friendsByAge.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()


  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "192.168.1.200")
      .option("port", 1978)
      .load()

    val friendsDF = lines.select(
      split(col("value"),",")(0).cast("integer").as("id"),
      split(col("value"),",")(1).as("name"),
      split(col("value"),",")(2).cast("integer").as("age"),
      split(col("value"),",")(3).cast("integer").as("numFriends")
    )

    //    friendsDF.printSchema()

//    val aggregationDF = friendsDF.select(aggFunction(col("numFriends")).as("numFriends"))

    val friendsByAgeAgg = friendsDF
      .groupBy(col("age"))
      .agg(
        aggFunction(col("numFriends")).as("num_Friends")
      )

    friendsByAgeAgg.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    friendsCount
  }

}
