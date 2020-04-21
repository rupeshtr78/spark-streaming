package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterMyFeed {


  val spark = SparkSession.builder()
    .appName("TwitterProject")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))


  def readTwitter():Unit = {
    val twitterStream : DStream[Status] = ssc.receiverStream(new TwitterMyFeedReceiver)
    val tweets = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val userScreenName = status.getUser.getScreenName
      val retweet = status.isRetweet
      val text = status.getText

      s"User: $username UserScreenName: $userScreenName ($followers followers) retweet: $retweet tweets: $text"

    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()

  }

  def readTwitterFiltered():Unit = {
    val twitterStream  = ssc.receiverStream(new TwitterMyFeedReceiver)
    .map { status => (
      status.getUser.getName,
      status.getId,
      status.getUser.getId,
      status.getUser.getFollowersCount,
      status.getUser.getScreenName,
      status.isRetweet,
      status.isFavorited,
      status.getFavoriteCount,

      status.getText )
      }

    twitterStream.filter(tweet =>
      tweet._7.equals(false)
    ).print()

  }




  def main(args: Array[String]): Unit = {

    readTwitterFiltered()
    ssc.start()
    ssc.awaitTermination()

  }

}
