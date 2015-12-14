package com.github.andr83.parsek.spark

import com.github.andr83.parsek._
import com.github.andr83.parsek.resources.ResourceFactory
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import resource._

import scala.collection.JavaConversions._

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

  lazy val resourceFactory = ResourceFactory()


  override def beforeJob(): Unit = {
    super.beforeJob()
    config.as[Option[Config]]("resources") foreach (resources => {
      val res = resources.root().unwrapped().keySet() map (key => {
        key -> resourceFactory.read(resources.getValue(key)).value
      })
      if (res.nonEmpty) {
        val newConfig = ConfigFactory.parseMap(mapAsJavaMap(Map("resources" -> mapAsJavaMap(res.toMap))))
        config = newConfig.withFallback(config)
      }
    })
    config = config.resolve()
  }

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
