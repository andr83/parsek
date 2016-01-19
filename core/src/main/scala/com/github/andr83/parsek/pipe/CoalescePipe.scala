package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * @author andr83
  */
case class CoalescePipe(fields: Seq[FieldPath], as: Option[FieldPath] = None) extends Pipe {

  def this(config: Config) = this(
    fields = config.as[Seq[String]]("fields").map(_.asFieldPath),
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  assert(fields.nonEmpty)

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = value match {
    case map: PMap => Some(get(map, fields.toList).map(map.updateValue(as.getOrElse(fields.head), _)).getOrElse(map))
    case _ => throw new IllegalStateException(s"Coalesce pipe accept only PMap value but given $value")
  }

  private def get(map: PMap, fields: List[FieldPath]): Option[PValue] = fields match {
    case head :: Nil => map.getValue(head)
    case head :: tail => map.getValue(head) match {
      case v: Some[PValue] => v
      case None => get(map, tail)
    }
    case _ => None
  }
}
