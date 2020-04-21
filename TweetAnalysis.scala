package com.rupesh

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TweetAnalysis {


  val spark = SparkSession.builder()
    .appName("TwitterProject")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

  case class Tweets(text: String,
                    tweetTime: Date
                   )


  def readTwitter() = {
    val twitterStream : DStream[Status] = ssc.receiverStream(new TwitterMyFeedReceiver)
    ssc.checkpoint("checkpoints")
    twitterStream.map( status => status.getText)

  }

  def tweetLengthCountbyTime() = {

    val tweetLength = readTwitter()
      .map(tweet => (tweet.length , 1))
      .window(Seconds(5))
      .reduce((x,y) => (x._1 + y._1 , x._2 + y._2))

    tweetLength.map( t =>  t._1 / t._2)
    }



  def tweetHashtag()  ={
    val tweetWords = readTwitter()
      .flatMap(tweet => tweet.split(" ") )

    val hashTag = tweetWords.filter(word => word.startsWith("#")).map(x => (x,1))

    val hashtagCount = hashTag.reduceByKeyAndWindow(_+_, _-_,Seconds(60),Seconds(10))

    val popularHashtag = hashtagCount.transform(rdd => rdd.sortBy(tuple => -tuple._2))


    popularHashtag
  }



  def main(args: Array[String]): Unit = {

    tweetHashtag().print()
    ssc.start()
    ssc.awaitTermination()


  }

}
