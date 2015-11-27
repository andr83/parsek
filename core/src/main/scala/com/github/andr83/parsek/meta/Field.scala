package com.github.andr83.parsek.meta

import com.github.andr83.parsek._
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * @author andr83
 */

sealed trait Field[T <: PValue] {
  val name: String
  var isRequired: Boolean = false

  def validate(value: PValue)(implicit errors: mutable.Buffer[FieldError]): Option[T]
}

sealed trait StringCase

object UpperCase extends StringCase

object LowerCase extends StringCase

trait ValidationError extends RuntimeException {
  val msg: String
}

case class RequiredFieldError(field: FieldType, cause: Throwable) extends RuntimeException

case class IllegalValueType(msg: String) extends ValidationError

case class FieldIsEmpty(msg: String) extends ValidationError

case class PatternNotMatched(msg: String) extends ValidationError

case class StringField(
  name: String, pattern: Option[Regex],
  stringCase: Option[StringCase]
) extends Field[PString] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PString] = {
    var res = value.value.toString

    if (res.trim == "") {
      return None
    }

    res = stringCase map {
      case LowerCase => res.toLowerCase
      case UpperCase => res.toUpperCase
    } getOrElse res

    val matches = pattern forall (_.pattern.matcher(res).matches())
    if (!matches) {
      throw PatternNotMatched(s"Field $name does not match pattern ${pattern.get.pattern}")
    }
    Some(res)
  }
}

case class IntField(
  name: String
) extends Field[PInt] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PInt] = Some(value match {
    case v: PInt => v
    case PLong(num) => num.toInt
    case PString(str) => str.toInt
    case _ => throw IllegalValueType(s"Field $name expect int value type but got $value")
  })
}

case class LongField(
  name: String
) extends Field[PLong] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PLong] = Some(value match {
    case v: PLong => v
    case PInt(num) => num.toLong
    case PString(str) => str.toLong
    case _ => throw IllegalValueType(s"Field $name expect long value type but got $value")
  })
}

case class MapField(
  name: String,
  fields: Option[Seq[FieldType]]
) extends Field[PMap] {
  isRequired = isRequired || fields.exists(_.exists(_.isRequired))

  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PMap] = value match {
    case PMap(map) if fields.isDefined =>
      val res = fields.get flatMap (f => map.get(f.name) match {
        case Some(v) =>
          Try(f.validate(v)) match {
            case Success(Some(validated)) => Some(f.name -> validated)
            case Success(None) =>
              checkIfRequired(f, FieldIsEmpty(s"Field ${f.name} is empty in $value"))
              None
            case Failure(error: RequiredFieldError) => throw error
            case Failure(error) =>
              checkIfRequired(f, error)
              None
          }
        case None =>
          checkIfRequired(f, FieldIsEmpty(s"Field ${f.name} is empty in $value"))
          None
      })
      if (res.isEmpty) None else Some(res.toMap)
    case map: PMap => Some(map)
    case _ => throw IllegalValueType(s"Field $name expect map value type but got $value")
  }

  private def checkIfRequired(f: FieldType, ex: Throwable)
      (implicit errors: mutable.Buffer[FieldError]): Unit = {
    if (f.isRequired) {
      throw RequiredFieldError(f, ex)
    }
    errors += ((f, ex))
  }
}

object Field {

  def apply(config: Config): FieldType = {
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    config.as[String]("type") match {
      //      case "String" => fakeConfig(config).as[StringField]("fakeRoot")
      case "Int" => fakeConfig(config).as[IntField]("fakeRoot")
      case "Long" => fakeConfig(config).as[LongField]("fakeRoot")
      case "Map" => fakeConfig(config).as[MapField]("fakeRoot")
    }
  }

  implicit val fieldConfigReader: ValueReader[FieldType] = ValueReader.relative(config => {
    Field.apply(config)
  })

  def fakeConfig(config: Config): Config = ConfigFactory.parseMap(Map("fakeRoot" -> config.root().unwrapped()))
}