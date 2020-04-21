package flinkscala

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object StreamingFlink {

  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val conf: Configuration = new Configuration()
    val env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // get socket  data
    val text = env.socketTextStream("192.168.1.200",1978)

    val counts: DataStream[(String, Int)] = text
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // print result
    counts.print()


    // lazy execution
    env.execute("Flink Streaming")

  }

}
