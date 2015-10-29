package com.github.andr83.parsek.spark

import com.github.andr83.parsek._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.IOUtils
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
      ) yield IOUtils.toString(in)) either match {
        case Right(content) => ConfigFactory.parseString(content)
        case Left(errors) => throw errors.head
      }
    } else {
      ConfigFactory.parseFile(path)
    }
  } text "Configuration file path. Support local and hdfs"

  override def job(): Unit = {
    val sources = config.getConfigListOpt("sources") map (_ map {
      case config: Config => Source(config)
    }) getOrElse (throw new IllegalStateException("Sources config should not be empty"))

    val rdd = sources.map(_(this)).reduce(_ ++ _)

    val outRdd = config.getObjectListOpt("pipes") map (Pipeline(_)) map (pipeline => {
      rdd.flatMap(pipeline.run(_)).flatMap {
        case PList(list) => list
        case value => List(value)
      }
    }) getOrElse rdd

    outRdd.collect().foreach(println)
  }
}
