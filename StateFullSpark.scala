package com.rupesh

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{Dataset, SparkSession}

object StateFullSpark {

  val spark = SparkSession.builder()
    .appName("Spark Stateful")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  case class SocialPostRecord(postType:String,count:Int ,storageUsed: Int)
  case class SocialPostBulk(postType:String,count:Int,totalStorageUsed:Int)
  case class AveragePostStorage(postType:String, averageStorage:Double)

  import spark.implicits._

  def readSocialUpdates(): Dataset[SocialPostRecord] = spark.readStream
      .format("socket")
      .option("host","192.168.1.200")
      .option("port",1978)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
      }

  def updateAverageStorage(
                          postType : String ,
                          group : Iterator[SocialPostRecord],
                          state : GroupState[SocialPostBulk]
                          ) : AveragePostStorage = {

    val previousBulk =
      if(state.exists) state.get
      else SocialPostBulk(postType,0,0)

    val totalAggData:(Int,Int) = group.foldLeft((0,0)){(currentData,record) =>
    val (currentCount , currentStorage) = currentData
      (currentCount + record.count , currentStorage + record.storageUsed)
    }

    val (totalCount , totalStorage) = totalAggData
    val newPostBulk = SocialPostBulk(postType , previousBulk.count + totalCount , previousBulk.totalStorageUsed + totalStorage)
    state.update(newPostBulk)

    AveragePostStorage(postType,newPostBulk.totalStorageUsed *1.0 /newPostBulk.count)
  }

  def getAvgPostUpdates() = {
    val socialStream = readSocialUpdates()

    val sqlbasedAveragePost= socialStream
      .groupByKey(_.postType)
      .agg(
        sum(col("count")).as("totalCount").as[Int] ,
        sum(col("storageUsed")).as("totalStorage").as[Int]
      )
      .selectExpr("key as postType","totalStorage/totalCount as avgStorage")

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageStorage)

    averageByPostType.writeStream
//      .format("console")
      .outputMode("update")
      .foreachBatch{(batch : Dataset[AveragePostStorage] , _:Long) =>
        batch.show()
      }
      .start()
      .awaitTermination()

  }


  def main(args: Array[String]): Unit = {
  getAvgPostUpdates()
  }

}
