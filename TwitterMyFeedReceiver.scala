package com.rupesh

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._

import scala.concurrent.Promise

class TwitterMyFeedReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY){

  import scala.concurrent.ExecutionContext.Implicits.global

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def simpleStatusListener = new StatusListener {

    override def onStatus(status: Status): Unit = store(status)
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
    override def onStallWarning(warning: StallWarning): Unit = ()
    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  override def onStart(): Unit = {

    val twitterStream = new TwitterStreamFactory("src/main/resources")
      .getInstance()
      .addListener(simpleStatusListener)
      .sample("en")

    val screenName = "pahayan78"
    val twitter = new TwitterFactory("src/main/resources").getInstance()
    val twitterFriends = twitter.getFriendsIDs(screenName, -1)



    val myFriendsId = twitterFriends.getIDs


    val myFollowersFilter = TwitterFollower.twitterFriends()
    val myFilterQuery = new FilterQuery()


    val myTwitterStream = {
      twitterStream.filter(myFilterQuery.follow(myFriendsId: _*))
    }

    twitterStreamPromise.success(myTwitterStream)
  }

  override def onStop(): Unit = twitterStreamFuture.foreach{ twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }

}

