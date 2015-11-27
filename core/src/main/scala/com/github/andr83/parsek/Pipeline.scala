package com.github.andr83.parsek

import com.typesafe.config._
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
 * @author andr83
 */
class Pipeline(pipes: Iterable[Pipe]) extends Serializable with LazyLogging {
  import PipeContext._

  def run(value: PValue)(implicit context: PipeContext): Option[PValue] = {
    try {
      nextPipe(pipes.iterator, value)
    } catch {
      case NonFatal(e) =>
        logger.error(e.toString, e)
        context.getCounter(ErrorGroup, e.getClass.getSimpleName) += 1
        None
    } finally {
      context.getCounter(InfoGroup, "ROW_COUNT") += 1
    }
  }

  private def nextPipe(it: Iterator[Pipe], value: PValue)(implicit context: PipeContext): Option[PValue] = if (it.hasNext) {
    context.row = value match {
      case map: PMap => map
      case _ => PMap.empty
    }
    val pipe = it.next()
    pipe.run(value) flatMap (res => nextPipe(it, res))
  } else Some(value)
}

object Pipeline {
  def apply(pipeConfigs: Iterable[ConfigObject]): Pipeline = {
    val pipes = pipeConfigs map (config => {
      val map = config.unwrapped()
      if (map.size() != 1) {
        throw new IllegalStateException("Pipe config should contain only one element.")
      }
      val (key, _) = map.head
      config.get(key) match {
        case conf: Config => Pipe(key, conf)
        case obj: ConfigObject => Pipe(key, obj.toConfig)
        case list: ConfigList => Pipe(key, ConfigFactory.parseMap(Map("config"->list.unwrapped())))
        case field: ConfigValue if field.valueType() == ConfigValueType.STRING =>
          val conf = ConfigFactory.parseMap(Map("field" -> field.unwrapped().toString))
          Pipe(key, conf)
        case _ => throw new IllegalStateException("Pipe config should be an object or string.")
      }
    })
    new Pipeline(pipes)
  }
}
