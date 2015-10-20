package com.github.andr83.parsek

import com.github.andr83.parsek.pipe.parser._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * @author andr83
 */
trait Pipe extends LazyLogging {
  val config: Config
  def run(value: PValue): Option[PValue]
}

object Pipe {
  def apply(name: String, config: Config): Pipe = if (name.startsWith("$")) name.substring(1) match {
    case "json" => JsonParser(config)
    case _ => throw new IllegalStateException(s"Unknown pipe $name")
  } else throw new IllegalArgumentException(s"Pipe name must begin with $$. Actual: $name")
}
