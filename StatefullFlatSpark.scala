package com.rupesh

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

object StatefullFlatSpark {

  val spark = SparkSession.builder()
    .appName("Statefull Flatmap Spark")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  import spark.implicits._

  case class SessionData(session : String , time: Long)
  case class SessionAvg(session : String, avgTime: Double)
  case class SessionAgg(session : String, avgTime: Double, maxTime: Long)

  def readSessionData(): Dataset[SessionData] =
  spark.readStream
    .format("socket")
    .option("host","192.168.1.200")
    .option("port",1978)
    .load()
    .select("value")
    .as[String]
    .map { line =>
      val tokens = line.split(",")
      val session = tokens(0)
      val time = tokens(1).toInt

      SessionData(session,time)
    }


  def updateFunction
  (n: Int)
  (session : String , group: Iterator[SessionData] , state : GroupState[List[SessionData]])
    : Iterator[SessionAgg]   = {
    group.flatMap{ record =>
      val lastwindow =
        if(state.exists) state.get
        else List()

      val windowLength = lastwindow.length
      val newWindows =
      if (windowLength >= n) lastwindow.tail :+ record
      else lastwindow :+ record
//      println(newWindows)

      state.update(newWindows)

      if (newWindows.length >= n) {
        val newAvg = newWindows.map(_.time).sum * 1.0 / n
        val newMax = newWindows.map(_.time).max
        Iterator(SessionAgg(session,newAvg ,newMax))
      }
      else
        Iterator()

    }

  }

  def avgSessionData(n:Int) = {
    readSessionData()
    .groupByKey(_.session)
    .flatMapGroupsWithState(OutputMode.Append , GroupStateTimeout.NoTimeout())(updateFunction(n))

  }

  def logSessionData(n:Int) = {
    val results = avgSessionData(n)
      results
      .writeStream
      .queryName("stateavgmax")
      .format("memory")
      .outputMode("append")
      .start()

    spark.sql(
      """
        |SELECT * FROM stateavgmax
        |""".stripMargin).show()
   }



  def main(args: Array[String]): Unit = {
    logSessionData(3)

  }

}