package com.github.andr83.parsek.spark.streaming

import com.github.andr83.parsek.spark.SparkJob
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author andr83
  */
abstract class StreamingJob extends SparkJob {

  var ssc: StreamingContext = null

  override lazy val sparkConfig = {
    val conf = newSparkConfig()
    conf.set("spark.driver.allowMultipleContexts", "true")
    maxRate foreach (rate=> {
      conf.set("spark.streaming.backpressure.enabled", "true")
      conf.set("spark.streaming.receiver.maxRate", rate.toString)
      conf.set("spark.streaming.kafka.maxRatePerPartition", rate.toString)
    })
    conf
  }

  var batchDuration = Seconds(1)
  var checkpointDirectory: Option[String] = None
  var maxRate: Option[Int] = None


  opt[String]("batchDuration") foreach {
    s => batchDuration = Seconds(s.toLong)
  } text "Streaming batch duration interval in seconds. Default 1 second."

  opt[String]("checkpointDir") foreach {
    f => checkpointDirectory = Some(f)
  } text "Checkpoint directory to store current progress."

  opt[Int]("maxRate") foreach {
    rate => maxRate = Some(rate)
  } text "Maximum receiver rate limit in terms of records / sec."

  def createContext(): StreamingContext = {
    ssc = new StreamingContext(sparkConfig, batchDuration)
    checkpointDirectory foreach ssc.checkpoint

    super.run()
    ssc
  }

  override def run(): Unit = {
    Logger.getLogger("kafka").setLevel(sparkLogLevel)

    val ssc = checkpointDirectory map (dir=> {
      StreamingContext.getOrCreate(dir, () => {
        createContext()
      }, hadoopConfig)
    }) getOrElse createContext()

    sys.ShutdownHookThread {
      logger.info("Gracefully stopping Spark Streaming Application")
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      logger.info("Application stopped")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
