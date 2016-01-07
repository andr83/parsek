package com.github.andr83.parsek.spark.streaming

import com.github.andr83.parsek.spark.{Sink, SparkPipeContext}
import com.github.andr83.parsek.{PList, PValue, Pipeline}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream


/**
  * @author andr83
  */
object ParsekStreamingJob extends StreamingJob {
  override def job(): Unit = {
    val sources = config.as[List[Config]]("sources") map StreamingSource.apply

    val inputStream: DStream[PValue] = {
      val streams = sources.map(_ (this))
      streams.tail.foldRight(streams.head)(_.union(_))
    }

    val pipesConfig = config.as[Option[List[Config]]]("pipes")

    implicit val context = SparkPipeContext(ssc.sparkContext)
    inputStream foreachRDD (rdd=> {
      val outRdd: RDD[PValue]  = (pipesConfig map(pipes=> {
        rdd mapPartitions (it => {
          val pipeline = Pipeline(pipes)
          it.flatMap(pipeline.run)
        }) flatMap {
          case PList(list) => list
          case value: PValue => List(value)
        }
      }) getOrElse rdd) cache()

      val sinks = config.as[List[Config]]("sinks") map Sink.apply
      sinks.foreach(_.sink(outRdd))

      logger.info("Counters:")
      context.getCounters.toSeq.sortWith(_._1.toString() < _._1.toString()) foreach { case (key, count) =>
        logger.info(s"$key: $count")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
