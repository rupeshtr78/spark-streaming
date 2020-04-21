package com.rupesh

import java.nio.charset.CodingErrorAction

import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Codec


object CassandraReadWrite {

  val spark = SparkSession.builder()
    .appName("Spark Cassandra")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .config("spark.cassandra.connection.host","192.168.1.200")
    .config("spark.cassandra.connection.port","9042")
    .config("spark.cassandra.auth.username","cassandra")
    .config("spark.cassandra.auth.password","cassandra")
    .getOrCreate()

  import spark.implicits._

  val UserSchema = StructType(Seq(
    StructField("userid",IntegerType,false),
    StructField("age",IntegerType,true),
    StructField("gender",StringType,true),
    StructField("occupation",StringType,true),
    StructField("zip",StringType,true)
      ))

  case class UserClass(
                      userid: Int,
                      age:Int,
                      gender:String,
                      occupation:String,
                      zip:String
                      )

//  user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

  def readUserData() = {
    spark.read
      .textFile("src/main/resources/ml-100k/u.user")
      .map(lines => {
       val user = lines.split('|')
       UserClass(user(0).toInt,user(1).toInt,user(2).toString,user(3).toString,user(4).toString)
      })
  }


  def writetoCassandra()={
    readUserData()
          .write
          .cassandraFormat("movieusers","hyper")
          .mode(SaveMode.Append)
          .save()
  }

  import spark.sqlContext.sql

  val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)

  val  convertString = udf((utfstring: String) =>
    utfstring.asInstanceOf[java.lang.String]
  )


  def readFromCassandra() = {



    spark.read
      .cassandraFormat("movieusers", "hyper")
      //      .schema(UserSchema)
      .load()
      .select(col("gender"))
      .withColumn("gender",convertString(col("gender")))
      .createOrReplaceTempView("users")


    sql("SELECT * FROM users").show()
  }


  def main(args: Array[String]): Unit = {
//    readUserData().show()
//    writetoCassandra()
    readFromCassandra()
  }

}

//create table hyper.movieusersvc (userId int PRIMARY KEY,age int,gender varchar,occupation varchar,zip varchar)