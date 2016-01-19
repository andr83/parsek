package com.github.andr83.parsek.spark.sink

import com.github.andr83.parsek._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD

/**
  * Sink is define destination of rdd. It can be stored to filesystem, db, kafka and etc
  *
  * @author andr83
  */
abstract class Sink extends LazyLogging {
  def sink(rdd: RDD[PValue]): Unit
}

object Sink {
  def apply(config: Config): Sink = {
    val sinkType = config.as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("Sink config must have type property"))
    val className = if (sinkType.contains(".")) sinkType
    else
      getClass.getPackage.getName + "." + sinkType.head.toUpper + sinkType.substring(1) + "Sink"
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[Sink]
  }
}
