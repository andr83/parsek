package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
abstract class TransformPipe(field: FieldPath, as: Option[FieldPath] = None) extends Pipe {
  val asField = as.getOrElse(field)

  def this(field: String) = this(field.split('.'))
  def this(field: String, as: Option[String]) = this(field.asFieldPath, as.map(_.asFieldPath))

  def this(config: Config) = this(
    field = config.as[String]("field").asFieldPath,
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = {
    value match {
      case map: PMap =>
        context.row = map
      case _ =>
    }
    context.path = field

    val res = transform(value, field)
    if (asField.isEmpty) {
      res
    } else res.map(resValue => value match {
      case _: PString => PMap.empty.updateValue(asField, resValue)
      case map: PMap => map.updateValue(asField, resValue)
      case _ => throw new IllegalArgumentException(
        s"String transform pipe accept only string input but ${value.getClass} given. Value: $value"
      )
    })
  }

  def transform(value: PValue, field: Seq[String])(implicit context: PipeContext): Option[PValue] = value match {
    case PString(raw) if field.isEmpty => transformString(raw)
    case map: PMap if field.nonEmpty => map.getValue(field.head) flatMap (fieldValue => transform(fieldValue, field.tail))
    case _ => throw new IllegalArgumentException(
      s"String transform pipe accept only string input but ${value.getClass} given. Value: $value"
    )
  }

  def transformString(str: String)(implicit context: PipeContext): Option[PValue]
}