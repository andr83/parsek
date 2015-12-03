package com.github.andr83.parsek.spark.sink

import com.github.andr83.parsek._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
abstract class Serializer(config: Config) extends LazyLogging {
  def write(value: PValue): Array[Byte]
}

object Serializer {
  def apply(config: Config): Serializer = {
    val sType = config.as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("Serializer config should have type property"))
    val className = if (sType.contains(".")) sType
    else
      "com.github.andr83.parsek.spark.sink.serializer." + sType.head.toUpper + sType.substring(1) + "Serializer"
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[Serializer]
  }
}
