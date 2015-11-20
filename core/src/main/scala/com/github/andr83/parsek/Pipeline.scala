package com.github.andr83.parsek

import com.typesafe.config.ConfigObject
import scaldi.Injector

import scala.collection.JavaConversions._

/**
 * @author andr83
 */
class Pipeline(pipes: Iterable[Pipe]) extends Serializable {
  def run(value: PValue): Option[PValue] = nextPipe(pipes.iterator, value)

  private def nextPipe(it: Iterator[Pipe], value: PValue): Option[PValue] = if (it.hasNext) {
    val pipe = it.next()
    pipe.run(value) flatMap (res => nextPipe(it, res))
  } else Some(value)
}

object Pipeline {
  def apply(configs: Iterable[ConfigObject])(implicit inj: Injector): Pipeline = {
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
