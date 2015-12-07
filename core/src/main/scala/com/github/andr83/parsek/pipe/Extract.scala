package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
case class Extract(config: Config) extends Pipe {
  val from: Seq[String] = config.as[String]("from").split('.')
  val to: Option[Seq[String]] = config.as[Option[String]]("to").map(_.split('.'))

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = value match {
    case PMap(root) =>
      Some(pop(value, from) match {
        case Some((v,m)) =>
          val res = if (m.nonEmpty) {
            root + (from.head -> PMap(m))
          } else root - from.head
          to.map(toField=> res.updateValue(toField, v)).getOrElse(res ++ v.asInstanceOf[PMap].value)
        case _ => root
      })
  }

  def pop(value: PValue, path: Seq[String]): Option[(PValue, Map[String, PValue])] = value match {
    case PMap(map) if path.length > 1 => map.get(path.head).flatMap(pop(_, path.tail)).map {
      case (v, child) if child.isEmpty => v -> (map - path.head)
      case (v, child) => v -> (map + (path.head -> child))
    }
    case PMap(map) => for (v <- map.get(path.head)) yield v -> (map - path.head)
    case PList(_) => throw new IllegalStateException(s"Can not extract values from list in path ${path.mkString(".")}")
    case _ => throw new IllegalStateException(s"Can not extract values in path ${path.mkString(".")}")
  }
}
