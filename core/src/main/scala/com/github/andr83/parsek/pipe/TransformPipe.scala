package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config

/**
 * @author andr83
 */
abstract class TransformPipe(config: Config) extends Pipe {
  val field = config.getStringOpt("field") map (_.split('.').toSeq)
  val asField = config.getStringOpt("as") map (_.split('.').toSeq) getOrElse field.getOrElse(Seq.empty[String])

  override def run(value: PValue)(implicit context: Context): Option[PValue] = {
    value match {
      case map: PMap =>
        context.row = map
      case _ =>
    }

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

  def transform(value: PValue, field: Option[Seq[String]] = None)(implicit context: Context): Option[PValue] = value match {
    case PString(raw) if field.isEmpty => transformString(raw)
    case map: PMap if field.isDefined => map.getValue(field.get) flatMap (fieldValue => transform(fieldValue))
    case _ => throw new IllegalArgumentException(
      s"String transform pipe accept only string input but ${value.getClass} given. Value: $value"
    )
  }

  def transformString(str: String)(implicit context: Context): Option[PValue]
}