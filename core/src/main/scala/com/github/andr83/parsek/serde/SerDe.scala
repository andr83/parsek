package com.github.andr83.parsek.serde

import com.github.andr83.parsek._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
trait Serializer {
  def write(value: PValue): Array[Byte]
}

trait Deserializer {
  def read(value: Array[Byte]): PValue
}

abstract class SerDe extends Serializer with Deserializer with LazyLogging

object SerDe {
  def apply(config: Config): SerDe = {
    val sType = config.as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("SerDe config should have type property"))
    val className = if (sType.contains(".")) sType
    else
      getClass.getPackage.getName + "." + sType.head.toUpper + sType.substring(1) + "SerDe"
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[SerDe]
  }
}