package com.github.andr83.parsek.meta

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.DateFormatter

//import com.github.andr83.parsek.ParsekConfig._
import com.github.nscala_time.time.Imports._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
//import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * @author andr83
 */

sealed trait Field[T <: PValue] {
  val name: String
  var as: Option[String] = None
  var isRequired: Boolean = false

  def asField = as.getOrElse(name)
  def validate(value: PValue)(implicit context: PipeContext): Option[T]
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
  override def validate(value: PValue)(implicit context: PipeContext): Option[PString] = {
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
  override def validate(value: PValue)(implicit context: PipeContext): Option[PInt] = Some(value match {
    case v: PInt => v
    case PLong(num) => num.toInt
    case PString(str) => str.toInt
    case _ => throw IllegalValueType(s"Field $name expect int value type but got $value")
  })
}

case class LongField(
  name: String
) extends Field[PLong] {
  override def validate(value: PValue)(implicit context: PipeContext): Option[PLong] = Some(value match {
    case v: PLong => v
    case PInt(num) => num.toLong
    case PString(str) => str.toLong
    case _ => throw IllegalValueType(s"Field $name expect long value type but got $value")
  })
}

case class DoubleField(
  name: String
) extends Field[PDouble] {
  override def validate(value: PValue)(implicit context: PipeContext): Option[PDouble] = Some(value match {
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
  override def validate(value: PValue)(implicit context: PipeContext): Option[PBool] = Some(value match {
    case v: PBool => v
    case PInt(num) if Seq(0, 1).contains(num) => num == 1
    case PLong(num) if Seq(0, 1).contains(num) => num == 1
    case PString(str) => str.toBoolean
    case _ => throw IllegalValueType(s"Field $name expect double value type but got $value")
  })
}

case class DateField(
  name: String,
  pattern: DateFormatter,
  toTimeZone: Option[DateTimeZone] = None
) extends Field[PDate] {
  override def validate(value: PValue)(implicit context: PipeContext): Option[PDate] = Some({
    val dt = pattern.parse(value)
    toTimeZone map (tz => PDate(dt.value.toDateTime(tz))) getOrElse dt
  })
}

case class RecordField(
  name: String,
  fields: Seq[FieldType]
) extends Field[PMap] {

  override def validate(value: PValue)(implicit context: PipeContext): Option[PMap] = value match {
    case PMap(map) =>
      val res = fields flatMap (f => map.get(f.name) match {
        case Some(v) =>
          Try(f.validate(v)) match {
            case Success(Some(validated)) => Some(f.asField -> validated)
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
    case _ => throw IllegalValueType(s"Field $name expect map value type but got $value")
  }

  private def checkIfRequired(f: FieldType, ex: Throwable)(implicit context: PipeContext): Unit = {
    if (f.isRequired) {
      throw RequiredFieldError(f, ex)
    }
    context.getCounter(PipeContext.InfoGroup, (ex.getClass.getSimpleName, f.name).toString()) += 1
  }
}

case class MapField(
  name: String,
  field: Option[FieldType]
) extends Field[PMap] {
  override def validate(value: PValue)(implicit context: PipeContext): Option[PMap] = value match {
    case PMap(map) =>
      val res = field map (f => map.flatMap{case (k,v) => f.validate(v).map(k->_)}) getOrElse map
      if (res.isEmpty) None else Some(res)
    case _ => throw IllegalValueType(s"Field $name expect map value type but got $value")
  }
}

case class ListField(
  name: String,
  field: Option[FieldType]
) extends Field[PList] {
  override def validate(value: PValue)(implicit context: PipeContext): Option[PList] = value match {
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

  def apply(cfg: Config): FieldType = {
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    val config = if (cfg.hasPath("name")) cfg else cfg.withFallback(Map("name" -> "NoName"))
    val field = config.as[String]("type") match {
      case "String" => config.fake().as[StringField](fakeKey)
      case "Int" => config.fake().as[IntField](fakeKey)
      case "Long" => config.fake().as[LongField](fakeKey)
      case "Double" => config.fake().as[DoubleField](fakeKey)
      case "Boolean" => config.fake().as[BooleanField](fakeKey)
      case "Date" => config.fake().as[DateField](fakeKey)
      case "Record" => config.fake().as[RecordField](fakeKey)
      case "Map" => config.fake().as[MapField](fakeKey)
      case "List" => config.fake().as[ListField](fakeKey)
      case fieldType => throw new IllegalStateException(s"Unsupported field type $fieldType")
    }

    field.as = config.as[Option[String]]("as")
    field.isRequired = config.as[Option[Boolean]]("isRequired").getOrElse(false)
    field.isRequired = isRequired(field)
    field
  }

  def isRequired(field: FieldType): Boolean = field.isRequired || (field match {
    case rf: RecordField => rf.fields exists isRequired
    case mf: MapField => mf.field exists isRequired
    case lf: ListField if lf.field.isDefined => isRequired(lf.field.get)
    case _ => false
  })
}