package com.github.andr83.parsek

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._

import scala.reflect.runtime.{universe => u}

/**
 * @author andr83
 */
trait Pipe extends LazyLogging with Serializable {
  def run(value: PValue)(implicit context: PipeContext): Option[PValue]
}

object Pipe {
  def apply(config: Config): Pipe = {
    val pipeName = config.as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException(s"Pipe config must have type property: $config"))
    val className = if (pipeName.contains(".")) pipeName else
      "com.github.andr83.parsek.pipe." + pipeName.head.toUpper + pipeName.substring(1)

    val configClass = classOf[Config]
    val pipe = for {
      c <- Class.forName(className).getConstructors
      if c.getParameterCount == 1
      p <- c.getParameters
      if p.getType == configClass
    } yield c.newInstance(config).asInstanceOf[Pipe]

    if (pipe.nonEmpty) pipe.head else
      throw new NoSuchMethodException(s"Cannot find constructor with argument $configClass for class $className")
  }
}
