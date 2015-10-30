package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config

/**
 * @author andr83
 */
abstract class TransformPipe(config: Config) extends Pipe {
  val field = config.getStringOpt("field") map(_.split("."))
  val as = config.getStringOpt("as")

  override def run(value: PValue): Option[PValue] = {
    val res = transform(value)
    for {
      resValue <- res
      asField <- as
    } yield value match {
      case _: PString => PMap(Map(asField -> resValue))
      case map: PMap => PMap(map.value + (asField -> resValue))
      case _ => throw new IllegalArgumentException(
        s"Parser pipe accept only string input but ${value.getClass} given. Value: $value"
      )
    }
  }
  
  def transform(value: PValue): Option[PValue] = value match {
    case PString(raw) if field.isEmpty => transformString(raw)
    case map: PMap if field.isDefined => getValue(map, field.get) flatMap transform
    case _ => throw new IllegalArgumentException(
      s"Parser pipe accept only string input but ${value.getClass} given. Value: $value"
    )
  }

  def getValue(obj: PMap, path: Seq[String]): Option[PValue] = {
    val (head :: tail) = path
    if (tail.isEmpty) {
      obj.value.get(head)
    } else obj.value.get(head) flatMap {
      case map: PMap => getValue(map, tail)
      case _ => throw new IllegalStateException(s"Failed extracting value from path $path in $obj")
    }
  }

  def transformString(raw: String): Option[PValue]
}