package com.github.andr83.parsek

import com.github.andr83.parsek.meta.RequiredFieldError
import com.typesafe.config._
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.control.NonFatal

/**
 * @author andr83
 */
class Pipeline(pipes: Pipe*) extends Serializable with LazyLogging {
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
  def apply(pipes: Seq[Config]): Pipeline = new Pipeline(pipes.map(Pipe.apply):_*)
}
