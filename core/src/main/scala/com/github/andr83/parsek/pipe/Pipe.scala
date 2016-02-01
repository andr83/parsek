package com.github.andr83.parsek.pipe

import com.github.andr83.parsek.{PValue, PipeContext}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._

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
      getClass.getPackage.getName + "." + pipeName.head.toUpper + pipeName.substring(1) + "Pipe"

    val configClass = classOf[Config]
    val pipe = for {
      c <- Class.forName(className).getConstructors
      if c.getParameterTypes.size == 1 && c.getParameterTypes.head == configClass
    } yield c.newInstance(config).asInstanceOf[Pipe]

    if (pipe.nonEmpty) pipe.head else
      throw new NoSuchMethodException(s"Cannot find constructor with argument $configClass for class $className")
  }
}
