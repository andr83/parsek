package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
case class Flatten(config: Config) extends Pipe {
  val field: Seq[String] = config.as[String]("field").split('.').toSeq

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = {
    val res = next(field, Map.empty[String, PValue], value)
    if (res.isEmpty) None else Some(PList(res))
  }

  def next(fields: Seq[String], current: Map[String, PValue], value: PValue): List[PValue] = if (fields.nonEmpty) {
    value match {
      case PList(list) => list flatMap (next(fields, current, _))
      case pm@PMap(map) =>
        val f = fields.head
        map.get(f) map (v => {
          next(fields.tail, current ++ (map - f), v)
        }) getOrElse List(pm)
      case v if fields.length == 1 => List(PMap(current + (fields.head -> v)))
      case _ => throw new IllegalStateException(s"Can not flatten value $value in path ${field.mkString(".")}")
    }
  } else {
    value match {
      case PList(list) => list map {
        case PMap(map) => PMap(current ++ map)
        case _ => throw new IllegalStateException(s"Can not flatten value $value in path ${field.mkString(".")}")
      }
      case _ => throw new IllegalStateException(s"Can not flatten value $value in path ${field.mkString(".")}")
    }
  }
}
