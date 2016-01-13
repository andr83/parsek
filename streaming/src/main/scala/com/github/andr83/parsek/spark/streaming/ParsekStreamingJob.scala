package com.github.andr83.parsek.spark.streaming

import com.github.andr83.parsek.PValue
import com.github.andr83.parsek.spark.streaming.pipe.DStreamPipe
import com.github.andr83.parsek.spark.{Sink, SparkPipeContext}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.streaming.dstream.DStream


/**
  * @author andr83
  */
object ParsekStreamingJob extends StreamingJob {
  val DefaultFlow = "default"

  implicit val context = SparkPipeContext(ssc.sparkContext)

  override def job(): Unit = {
    val sourcesByFlow = config.as[List[Config]]("sources")
      .groupBy(_.as[Option[String]]("flow") getOrElse DefaultFlow)
      .mapValues(_.map(StreamingSource.apply))

    var streams: Map[String, DStream[PValue]] = sourcesByFlow.mapValues(sources => {
      val streams = sources.map(_ (this))
      streams.tail.foldRight(streams.head)(_.union(_))
    })

    val pipeConfigs = config.as[Option[List[Config]]]("pipes") getOrElse List.empty[Config]

    streams = nextPipe(pipeConfigs, streams)

    val sinkConfigs = config.as[List[Config]]("sinks") groupBy (_.as[Option[String]]("flow") getOrElse DefaultFlow)
    val sinkFlows = sinkConfigs.keySet

    streams filterKeys sinkFlows.contains foreach {
      case (flow, stream) => stream.foreachRDD(rdd => {
        val sinks = sinkConfigs.get(flow).get map Sink.apply
        val cachedRdd = if (sinks.size > 1) rdd.cache() else rdd
        sinks.foreach(_.sink(cachedRdd))
        logger.info(s"Flow $flow counters:")
        context.getCounters.toSeq.sortWith(_._1.toString() < _._1.toString()) foreach { case (key, count) =>
          logger.info(s"$key: $count")
        }
      })
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def nextPipe(pipes: List[Config], streams: Map[String, DStream[PValue]]): Map[String, DStream[PValue]] = pipes match {
    case head :: tail =>
      nextPipe(tail, runPipe(head, streams))
    case Nil => streams
  }

  def runPipe(pipeConfig: Config, streams: Map[String, DStream[PValue]]): Map[String, DStream[PValue]] = {
    val pipe = DStreamPipe(pipeConfig)
    val flow = pipeConfig.as[Option[String]]("flow") getOrElse DefaultFlow
    if (!streams.contains(flow)) {
      throw new IllegalStateException(s"Flow $flow is unavailable. Please check configuration.")
    }
    // doing in this ugly way because streams ++ pipe.run not work
    streams.flatMap {
      case (k, v) if k == flow => Map(k -> v) ++ pipe.run(k, v)
      case (k, v) => Map(k -> v)
    }
  }

}
