package com.github.andr83.parsek

import com.github.andr83.parsek.meta.RequiredFieldError
import com.typesafe.config._
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
 * @author andr83
 */
class Pipeline(pipes: Iterable[Pipe]) extends Serializable with LazyLogging {
  import PipeContext._

  def run(value: PValue)(implicit context: PipeContext): List[PValue] = {
    try {
      val res = nextPipe(pipes.iterator, value)
      context.getCounter(InfoGroup, "OUTPUT_ROWS") += res.length
      res
    } catch {
      case NonFatal(ex) =>
        val e = ex match {
          case error: RequiredFieldError => error.cause
          case _ => ex
        }
        logger.error(e.toString, e)
        if (context.path.isEmpty) {
          context.getCounter(ErrorGroup, e.getClass.getSimpleName) += 1
        } else {
          context.getCounter(ErrorGroup, (e.getClass.getSimpleName, context.path.mkString(".")).toString()) += 1
        }
        List()
    } finally {
      context.getCounter(InfoGroup, "INPUT_ROWS") += 1
    }
  }

  private def nextPipe(it: Iterator[Pipe], value: PValue)(implicit context: PipeContext): List[PValue] = if (it.hasNext) {
    context.path = Seq.empty[String]
    context.row = value match {
      case map: PMap => map
      case _ => PMap.empty
    }
    val pipe = it.next()
    pipe.run(value) map {
      case PList(list) => list flatMap(nextPipe(it, _))
      case pipeResult => nextPipe(it, pipeResult)
    } getOrElse List.empty[PValue]
  } else value match {
    case PList(list) => list
    case _ => List(value)
  }
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

  def apply(pipe: Pipe*): Pipeline = new Pipeline(pipe)
}
