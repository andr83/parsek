package com.github.andr83.parsek

import com.typesafe.config.ConfigObject
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._

/**
 * @author andr83
 */
class Pipeline(pipes: Iterable[Pipe]) extends Serializable with LazyLogging {
  import Context._
  implicit val context = new Context()

  def run(value: PValue): Option[PValue] = {
    try {
      nextPipe(pipes.iterator, value)
    } catch {
      case e: Exception =>
        logger.error(e.toString, e)
        context.getCounter(ErrorGroup, e.getClass.getSimpleName) inc()
        None
    } finally {
      context.getCounter(InfoGroup, "LINES") inc()
    }
  }

  private def nextPipe(it: Iterator[Pipe], value: PValue): Option[PValue] = if (it.hasNext) {
    context.row = value match {
      case map: PMap => map
      case _ => PMap.empty
    }
    val pipe = it.next()
    pipe.run(value) flatMap (res => nextPipe(it, res))
  } else Some(value)
}

object Pipeline {
  def apply(configs: Iterable[ConfigObject]): Pipeline = {
    val pipes = configs map (config => {
      val map = config.unwrapped()
      if (map.size() != 1) {
        throw new IllegalStateException("Pipe config should contain only one element.")
      }
      val (key, _) = map.head
      Pipe(key, config.toConfig.getConfig(key))
    })
    new Pipeline(pipes)
  }
}
