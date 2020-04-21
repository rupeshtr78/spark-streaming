package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.TwitterFactory

object TwitterFollower {

  val spark = SparkSession.builder()
    .appName("TwitterFollower")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .master("local[*]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

  def twitterFriends() = {
    val screenName = "pahayan78"
    val twitter = new TwitterFactory("src/main/resources").getInstance()
    val twitterFriendsId = twitter.getFriendsIDs(screenName, -1).getIDs

    twitterFriendsId

    //    println("Number of Following" , twitterFriendsId.size)


  def twitterFollowersDetails: Unit ={

          val myFollowerNames = twitterFriendsId.take(10).map { id =>
            val screenName = twitter.showUser(id).getScreenName
            val gId = twitter.showUser(id).getId
             (id,gId,screenName)
          }

      myFollowerNames.foreach(println)
  }

  }

  def main(args: Array[String]): Unit = {

    twitterFriends()

  }


}
