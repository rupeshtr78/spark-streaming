package elastisearch

import java.sql
import java.text.SimpleDateFormat
import java.sql.{Date, Timestamp}
import java.util.Date

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}

object SparkKafkaElastiSearch {

  val spark = SparkSession.builder()
    .appName("Spark Kafka ElastiSearch")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .config("spark.es.nodes","192.168.1.200")
    .config("spark.es.port","9200")
    .config("es.index.auto.create", "true")
    .getOrCreate()

  def readKafka() ={
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.181:9092")
      //      .option("subscribe","streams-spark-input")
      .option("assign", "{\"streams-spark-input3\":[0]}")
      .load()


  }

  val regexPatterns = Map(
    "ddd" -> "\\d{1,3}".r,
    "ip" -> """s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"""".r,
    "client" -> "(\\S+)".r,
    "user" -> "(\\S+)".r,
    "dateTime" -> "(\\[.+?\\])".r,
    "datetimeNoBrackets" -> "(?<=\\[).+?(?=\\])".r,
    "request" -> "\"(.*?)\"".r,
    "status" -> "(\\d{3})".r,
    "bytes" -> "(\\S+)".r,
    "referer" -> "\"(.*?)\"".r,
    "agent" -> """\"(.*)\"""".r
  )

//  "agent" -> "\"(.*?)\"".r

  def parseLog(regExPattern: String) = udf((url: String) =>
       regexPatterns(regExPattern).findFirstIn(url) match
          {
      case Some(parsedValue) => parsedValue
      case None => "unknown"
       }
  )


  //  val requestRegex = "(\\S+)"

  import spark.implicits._

  def parseKafka() ={
    readKafka()
      .select(col("topic").cast(StringType),
              col("offset"),
              col("value").cast(StringType))
//      .withColumn("client",regexp_extract(col("value"),requestRegex,0))
      .withColumn("user", parseLog("user")($"value"))
      .withColumn("dateTime", parseLog("datetimeNoBrackets")($"value"))
      .withColumn("request", parseLog("request")($"value"))
      .withColumn("agent", parseLog("agent")($"value"))
      .withColumn("status", parseLog("status")($"agent"))

  }

  case class logsDate(offset: Double,
                      status: Double,
                      user: String,
                      request: String,
                      dateTime: Timestamp)

  val DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss ZZZZ"
  val dateFormat = new SimpleDateFormat(DATE_FORMAT)

  val  convertStringToDate = udf((dateString: String) =>
    dateFormat.parse(dateString).getTime
  )

  val  convertStringToDate2 = udf((dateString: String) =>
    new java.sql.Date(dateFormat.parse(dateString).getTime)
  )



  def parseDate() = {
    parseKafka().select(col("offset").cast("double"),
      col("status").cast("double"),
      col("user"),col("request"),
      col("dateTime"))
//      to_timestamp(col("dateTime"),"dd/MMM/yyyy:HH:mm:ss ZZZZ").as("to_date"))
      .withColumn("dateTime", convertStringToDate2(col("dateTime")))
      .as[logsDate]
//      .writeStream
//      .start()
//      .awaitTermination()
//      .printSchema()      .writeStream
    ////      .format("console")
    ////      .outputMode("append")
    ////      .start()
    ////      .awaitTermination()
//
}

  def writeKafkaElastiSearch() = {
    parseDate()
      .writeStream
      .foreachBatch { (batch: Dataset[logsDate], batchId: Long) =>
        batch.select(col("*"))
          .write
          .format("org.elasticsearch.spark.sql")
          .option("checkpointLocation", "checkpoint")
          .mode(SaveMode.Append)
          .save("dblkafkastream/dblkafkatype")
      }
      .start()
      .awaitTermination()

  }



  def main(args: Array[String]): Unit = {
     writeKafkaElastiSearch()

  }

}


