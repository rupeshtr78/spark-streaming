package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DstreamWindow {

  val spark = SparkSession.builder()
    .appName("DSteam Window")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))


  case class Person(
                     id: Int,
                     age: Int,
                     friends: Int
                   )



  def readLines(): DStream[Person] = ssc .socketTextStream("192.168.1.200",1978).map { line =>
    val tokens = line.split(",")
    Person(
      tokens(0).toInt,
      tokens(1).toInt,
      tokens(2).toInt
    )
  }

  def readLinesByWindow() = readLines().window(Seconds(10))

  def readBySlidingWindow() = readLines().window(Seconds(8),Seconds(4))

  ssc.checkpoint(directory="checkpoint")

  def countbyWindow() = readLines().countByWindow(Seconds(10),Seconds(5))

  def readMap() = readLines().map{
    person =>Tuple2(person.age,person.friends)
  }

  def reduceBykey()  = readMap().reduceByKey(_+_)

  def reduceByKeyWindow() = {
    readMap().reduceByKeyAndWindow(_ + _,_-_, Seconds(4), Seconds(4))
  }

  def reduceByWindow() = {
    readMap()
       .map(_._2)
//      .map(age => (age,1))
      .reduceByWindow(_+_,Seconds(10),Seconds(5))
  }


//  def reduceBYWindow = readLines().map





  def main(args: Array[String]): Unit = {
    reduceByWindow().print()
    ssc.start()
    ssc.awaitTermination()
  }

}
