package com.github.andr83.parsek.meta

import com.github.andr83.parsek._
import com.github.nscala_time.time.Imports._
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import org.joda.time.format.DateTimeFormatter

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * @author andr83
 */

sealed trait Field[T <: PValue] {
  val name: String
  var as: Option[String] = None
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
  name: String,
  pattern: Option[Regex] = None,
  stringCase: Option[StringCase] = None
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

case class DoubleField(
  name: String
) extends Field[PDouble] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PDouble] = Some(value match {
    case v: PDouble => v
    case PInt(num) => num.toDouble
    case PLong(num) => num.toDouble
    case PString(str) => str.toDouble
    case _ => throw IllegalValueType(s"Field $name expect double value type but got $value")
  })
}

case class BooleanField(
  name: String
) extends Field[PBool] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PBool] = Some(value match {
    case v: PBool => v
    case PInt(num) if Seq(0, 1).contains(num) => num == 1
    case PLong(num) if Seq(0, 1).contains(num) => num == 1
    case PString(str) => str.toBoolean
    case _ => throw IllegalValueType(s"Field $name expect double value type but got $value")
  })
}

case class DateField(
  name: String,
  pattern: DateTimeFormatter,
  toTimeZone: Option[DateTimeZone] = None
) extends Field[PDate] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PDate] = Some(value match {
    case v: PDate => v
    case PString(str) =>
      val dt = pattern.parseDateTime(str)
      val res = toTimeZone map (tz => dt.toDateTime(tz)) getOrElse dt
      res
    case _ => throw IllegalValueType(s"Field $name expect date value type but got $value")
  })
}

case class TimestampField(
  name: String,
  timeZone: Option[DateTimeZone]
) extends Field[PDate] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PDate] = Some(value match {
    case v: PDate => v
    case PInt(num) => parse(num * 1000L)
    case PLong(num) => parse(if (num >= 100000000000L) num else num * 1000)
    case PString(str) =>
      val num = str.toLong
      parse(if (num >= 100000000000L) num else num * 1000)
    case _ => throw IllegalValueType(s"Field $name expect date value type but got $value")
  })

  def parse(ts: Long): DateTime = {
    val dt = new DateTime(ts)
    timeZone map (tz => dt.toDateTime(tz)) getOrElse dt
  }
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
            case Success(Some(validated)) => Some(f.as.getOrElse(f.name) -> validated)
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

case class ListField(
  name: String,
  field: Option[FieldType]
) extends Field[PList] {
  override def validate(value: PValue)
      (implicit errors: mutable.Buffer[FieldError]): Option[PList] = value match {
    case PList(list) =>
      val res = field map (f => list.flatMap(v => f.validate(v))) getOrElse list
      if (res.isEmpty) None else as match {
        case Some(asField) => Some(PList(res.map(v=>PMap(Map(asField->v)))))
        case None => Some(PList(res))
      }
    case _ => throw IllegalValueType(s"Field $name expect list value type but got $value")
  }
}

object Field {

  def apply(config: Config): FieldType = {
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    val field = config.as[String]("type") match {
      case "String" => fakeConfig(config).as[StringField]("fakeRoot")
      case "Int" => fakeConfig(config).as[IntField]("fakeRoot")
      case "Long" => fakeConfig(config).as[LongField]("fakeRoot")
      case "Double" => fakeConfig(config).as[DoubleField]("fakeRoot")
      case "Boolean" => fakeConfig(config).as[BooleanField]("fakeRoot")
      case "Date" => fakeConfig(config).as[DateField]("fakeRoot")
      case "Timestamp" => fakeConfig(config).as[TimestampField]("fakeRoot")
      case "Map" => fakeConfig(config).as[MapField]("fakeRoot")
      case "List" => fakeConfig(config).as[ListField]("fakeRoot")
    }

    field.as = config.as[Option[String]]("as")
    field.isRequired = config.as[Option[Boolean]]("isRequired").getOrElse(false)
    field.isRequired = isRequired(field)
    field
  }

  def isRequired(field: FieldType): Boolean = field.isRequired || (field match {
    case mf: MapField if mf.fields.isDefined => mf.fields.get exists isRequired
    case lf: ListField if lf.field.isDefined => isRequired(lf.field.get)
    case _ => false
  })

  def fakeConfig(config: Config): Config = ConfigFactory.parseMap(Map("fakeRoot" -> config.root().unwrapped()))
}