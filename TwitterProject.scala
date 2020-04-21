package com.rupesh

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterProject {


  val spark = SparkSession.builder()
    .appName("TwitterProject")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

  case class Tweets(username: String,
                    followers: Long,
                    text: String,
                    tweetTime: Timestamp
                   )


  def readTwitter():Unit = {
    val twitterStream : DStream[Status] = ssc.receiverStream(new TwitterReceiver)
    val tweets = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val userId = status.getUser.getScreenName
      val text = status.getText.length
      val tweetTime = status.getCreatedAt

      s"User $username $userId ($followers followers) tweets: $text Time: $tweetTime"

    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    readTwitter()

  }

}
