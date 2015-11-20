package com.github.andr83.parsek.spark

import com.github.andr83.parsek._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.rdd.RDD

/**
 * @author andr83
 */
abstract class Sink(config: Config) extends LazyLogging {
  def sink(rdd: RDD[PValue]): Unit
}

object Sink {
  def apply(config: Config): Sink = {
    val sourceType = config.getStringOpt("type")
      .getOrElse(throw new IllegalStateException("Source config should have type property"))
    val className = if (sourceType.contains(".")) sourceType
    else
      "com.github.andr83.parsek.spark.sink." + sourceType.head.toUpper + sourceType.substring(1) + "Sink"
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[Sink]
  }
}
