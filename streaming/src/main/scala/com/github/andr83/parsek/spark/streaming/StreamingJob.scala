package com.github.andr83.parsek.spark.streaming

import com.github.andr83.parsek.spark.SparkJob
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author andr83
  */
abstract class StreamingJob extends SparkJob {

  lazy val ssc = new StreamingContext(sparkConfig, batchDuration)

  var batchDuration = Seconds(1)

  opt[String]("batchDuration") foreach {
    s => batchDuration = Seconds(s.toLong)
  } text "Streaming batch duration interval in seconds. Default 1 second."
}
