package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
case class ZipValues(config: Config) extends Pipe {
  val field: Seq[String] = config.as[Option[String]]("field").map(_.split('.').toSeq).getOrElse(Seq.empty[String])
  val keyField: String = config.as[String]("keyField")
  val valueField: String = config.as[String]("valueField")

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = Some(zip(value, field))

  def zip(value: PValue, path: Seq[String]): PValue = if (path.isEmpty) zip(value) else value match {
    case PMap(map) =>
      PMap(map.get(path.head)
        .map(v=> path.head -> zip(v, path.tail))
        .map(map + _)
        .getOrElse(map))
    case PList(list) => list.map(zip(_, path))
    case _ => throw new IllegalStateException(s"Can not zip values in path ${field.mkString(".")}")
  }

  def zip(value: PValue): PValue = value match {
    case PList(list) =>
      PMap(list.flatMap {
        case PMap(map) =>
          for{
            k <- map.get(keyField)
            v <- map.get(valueField)
          } yield k.value.toString -> v
        case _ => throw new IllegalStateException(s"Can not zip values in path ${field.mkString(".")}")
      }.toMap)
    case _ => throw new IllegalStateException(s"Can not zip values in path ${field.mkString(".")}")
  }
}
