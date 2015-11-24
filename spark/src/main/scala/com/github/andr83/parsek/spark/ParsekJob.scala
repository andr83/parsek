package com.github.andr83.parsek.spark

import com.github.andr83.parsek._
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import resource._

/**
 * @author andr83
 */
object ParsekJob extends SparkJob {

  var config = ConfigFactory.empty()

  opt[String]('c', "config") required() foreach { path =>
    config = if (path.startsWith("hdfs://")) {
      (for (
        in <- managed(fs.open(path))
      ) yield IOUtils.toString(in)).either match {
        case Right(content) => ConfigFactory.parseString(content)
        case Left(errors) => throw errors.head
      }
    } else {
      ConfigFactory.parseFile(path)
    }
  } text "Configuration file path. Support local and hdfs"

  override def job(): Unit = {
    val sources = config.as[List[Config]]("sources") map Source.apply
    val rdd: RDD[PValue] = sources.map(_(this)).reduce(_ ++ _)

    val outRdd: RDD[PValue] = (config.as[Option[List[Config]]]("pipes") map (pipes => {
      rdd mapPartitions (it => {
        val pipeline = Pipeline(pipes.map(_.root()))
        it.flatMap(pipeline.run)
      }) flatMap {
        case PList(list) => list
        case value: PValue => List(value)
      }
    }) getOrElse rdd) cache()

    val sinks = config.as[List[Config]]("sinks") map Sink.apply
    sinks.foreach(_.sink(outRdd))
  }
}
