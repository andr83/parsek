package com.github.andr83.parsek

import com.typesafe.config.{ConfigValue, Config}
import scala.collection.JavaConversions._

/**
 * @author andr83
 */
case class Pipeline(pipes: Iterable[Pipe]) {
  def run(value: PValue): Option[PValue] = nextPipe(pipes.iterator, value)

  private def nextPipe(it: Iterator[Pipe], value: PValue): Option[PValue] = if (it.hasNext) {
    val pipe = it.next()
    pipe.run(value) flatMap(res => nextPipe(it, res))
  } else Some(value)
}

object Pipeline {
  def apply(config: Config): Pipeline = {
    val pipes = config.entrySet() map {
      case entry => Pipe(entry.getKey, entry.getValue.asInstanceOf[Config])
    }
    Pipeline(pipes)
  }
}
