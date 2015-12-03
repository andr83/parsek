package com.github.andr83.parsek.spark.sink.serializer

import java.io.StringWriter

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.github.andr83.parsek.spark.sink.Serializer
import com.opencsv.CSVWriter
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.control.NonFatal

/**
 * @author andr83
 */
class DelimitedSerializer(config: Config) extends Serializer(config) {
  val fields: List[FieldType] =
    try {
      config.as[List[String]]("fields") map (f => StringField(f))
    } catch {
      case _: Throwable => config.as[List[FieldType]]("fields")
    }

  val delimiter: Char = config.as[Option[Char]]("delimiter").getOrElse(',')
  val enclosure: Char = config.as[Option[Char]]("enclosure").getOrElse('"')
  val escape: Char = config.as[Option[Char]]("escape").getOrElse('\\')
  val listDelimiter = config.as[Option[String]]("listDelimiter").getOrElse("|")
  val mapFieldDelimiter = config.as[Option[String]]("mapFieldDelimiter").getOrElse(":")
  val nullValue = config.as[Option[String]]("nullValue").getOrElse("")

  val timeFormatter = DateFormatter(config.as[Option[String]]("timeFormat"))

  lazy val jsonSerializer = new JsonSerializer(config)

  override def write(value: PValue): Array[Byte] = value match {
    case PMap(map) =>
      try {
        val res = fields.map(f => map.get(f.writeName) match {
          case Some(v) => convert(v, f, 2)
          case None => nullValue
        })
        val writer = new StringWriter()
        val csvWriter = new CSVWriter(writer, delimiter, enclosure, escape, "")
        csvWriter .writeNext(res.toArray, false)
        writer.toString.asBytes
      } catch {
        case NonFatal(ex) =>
          logger.error(ex.toString, ex)
          Array.empty[Byte]
      }
    case _ => Array.empty[Byte]
  }

  def convert(value: PValue, field: FieldType, level: Int = 2): String = field match {
    case f: RecordField =>
      val map = value match {
        case PMap(m) => m
        case _ => throw new IllegalStateException(s"Can not serialize $value as Record ${f.name}")
      }
      f.fields map (innerField => {
        val res = map.get(innerField.writeName) map (convert(_, innerField, level + 1))
        res.getOrElse(nullValue)
      }) mkString getDelimiter(level)
    case f: MapField =>
      val map = value match {
        case PMap(m) => m
        case _ => throw new IllegalStateException(s"Can not serialize $value as Record ${f.name}")
      }
      (f.field map (innerField => {
        map.mapValues(convert(_, innerField, level + 2))
      })).getOrElse(map.mapValues(valueToString(_, level + 2))) map {
        case (k, v) => Array(k, v).mkString(getDelimiter(level + 1))
      } mkString getDelimiter(level)
    case f: ListField =>
      val list = value match {
        case PList(l) => l
        case _ => throw new IllegalStateException(s"Can not serialize $value as List ${f.name}")
      }
      f field match {
        case Some(innerField) => list.map(convert(_, innerField, level + 1)).mkString(getDelimiter(level))
        case None => list.map(valueToString(_, level + 1)) mkString getDelimiter(level)
      }
    case _ => valueToString(value, level)
  }

  def valueToString(value: PValue, level: Int = 2): String = value match {
    case PString(str) => if (str.trim.isEmpty) nullValue else str
    case PInt(num) => num.toString
    case PLong(num) => num.toString
    case PDouble(num) => num.toString
    case PBool(b) => if (b) "true" else "false"
    case PDate(time) => valueToString(timeFormatter.format(time))
    case list: PList => compact(render(jsonSerializer.convertToJson(list)))
    case map: PMap => compact(render(jsonSerializer.convertToJson(map)))
  }

  def getDelimiter(level: Int): String = if (level == 1) {
    delimiter.toString
  } else if (level == 2) {
    listDelimiter
  } else if (level == 3) {
    mapFieldDelimiter
  } else if (level < 25) {
    level.toChar.toString
  } else throw new IllegalStateException("Exceed the maximum level 24 of nesting for csv serializer")
}
