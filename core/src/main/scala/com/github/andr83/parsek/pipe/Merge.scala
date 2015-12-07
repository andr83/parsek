package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
case class Merge(config: Config) extends Pipe {
  val (path, fields) = {
    val fs = config.as[List[String]]("fields")
    val parts = fs.head.split('.')
    val path = parts.take(parts.length - 1)
    val res = for{
      f <- fs
      ps = f.split('.')
      pr = ps.take(ps.length - 1)
    } yield if (pr sameElements path) ps.last
    else throw new IllegalStateException(s"Merged can be only fields on the same level")
    path -> res
  }
  
  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = merge(value, path)

  def merge(value: PValue, in_path: Seq[String]): Option[PValue] = value match {
    case PMap(map) if in_path.nonEmpty => map.get(in_path.head).flatMap(merge(_, in_path.tail))
    case PMap(map) => Some(map ++ fields.map(f=> f -> map.get(f)).collect {
      case (k, Some(v)) => k -> v
    })
    case PList(list) =>
      val res = list.flatMap(merge(_, in_path))
      if (res.isEmpty) None else Some(PList(res))
    case v => throw new IllegalStateException(s"Can not get value in path ${path.mkString(".")} for merge")
  }
}
