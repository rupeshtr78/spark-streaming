package elastisearch


import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
//import org.elasticsearch.spark.sql._

import SparkKafkaElastiSearch._

object ElastiSearchStream {

  val spark = SparkSession.builder()
    .appName("ElastiSearch")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .config("spark.es.nodes","192.168.1.200")
    .config("spark.es.port","9200")
    .config("es.index.auto.create", "true")
    .getOrCreate()


  import spark.implicits._

  def writeKafkaElastiSearch() = {
    parseDate()
      .writeStream
      .foreachBatch { (batch: Dataset[logsDate], batchId: Long) =>
        batch.select(col("*"))
          .write
          .format("org.elasticsearch.spark.sql")
          .option("checkpointLocation", "checkpoint")
          .mode(SaveMode.Append)
          .save("kafkastream/kafkatype")
      }
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    writeKafkaElastiSearch()
  }

}
