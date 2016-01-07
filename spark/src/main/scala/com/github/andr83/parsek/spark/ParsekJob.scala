package com.github.andr83.parsek.spark

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.rdd.RDD

/**
  * @author andr83
  */
object ParsekJob extends SparkJob {

  override def job(): Unit = {
    val startTime = System.currentTimeMillis()
    val sources = config.as[List[Config]]("sources") map Source.apply
    val rdd: RDD[PValue] = sources.map(_ (this)).reduce(_ ++ _)

    implicit val context = SparkPipeContext(sc)
    val outRdd: RDD[PValue] = (config.as[Option[List[Config]]]("pipes") map (pipes => {
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

    logger.info(s"Duration: ${System.currentTimeMillis() - startTime}ms")
    logger.info("Counters:")
    context.getCounters.toSeq.sortWith(_._1.toString() < _._1.toString()) foreach { case (key, count) =>
      logger.info(s"$key: $count")
    }
  }
}
