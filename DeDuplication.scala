package com.rupesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._

object DeDuplication extends App {

  val spark = SparkSession.builder()
    .appName("DeDuplication")
    .master("local")
    .config("spark.testing.memory",2147480000) //471859200
    .config("spark.driver.memory","4g")
    .config("spark.executor.memory","4g")
    .getOrCreate()

  import spark.implicits._
  implicit val sqlContext = spark.sqlContext

  val source = MemoryStream[(Int,Int)]

  val ids = source.toDS.toDF("time","id")
    .withColumn("time",$"time" cast("timestamp"))
    .withWatermark("time","2 second")
    .dropDuplicates("id")
    .withColumn("time",$"time" cast("long"))

//  println(ids.queryExecution.analyzed.numberedTreeString)

  // Publish duplicate records
  source.addData(1 -> 1)
  source.addData(2 -> 1)
  source.addData(3 -> 2)
  source.addData(4 -> 1)
  source.addData(5 -> 2)
  source.addData(6 -> 1)
  source.addData(7 -> 2)

  val q = ids.
    writeStream.
    format("memory").
    queryName("dups").
    outputMode(OutputMode.Append).
    trigger(Trigger.ProcessingTime(1.seconds)).
    option("checkpointLocation", "checkpoint-dir"). // <-- use checkpointing to save state between restarts
    start

  q.processAllAvailable()

  spark.table("dups").show()


  // Check out the internal state
  println(q.lastProgress.stateOperators(0).prettyJson)
  q.stop()


}
