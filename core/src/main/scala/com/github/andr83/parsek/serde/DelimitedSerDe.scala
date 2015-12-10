package com.github.andr83.parsek.serde

import java.io.StringWriter

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.opencsv.{CSVParser, CSVWriter}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.control.NonFatal

/**
 * @author andr83
 */
trait DelimitedSerDeTrait extends SerDe {
  val fields: List[FieldType] =
    try {
      config.as[List[String]]("fields") map (f => StringField(f))
    } catch {
      case _: Throwable => config.as[List[FieldType]]("fields")
    }

  val enclosure: Char = config.as[Option[Char]]("enclosure").getOrElse('"')
  val escape: Char = config.as[Option[Char]]("escape").getOrElse('\\')
  val delimiter: Char = config.as[Option[Char]]("delimiter").getOrElse(1)
  val listDelimiter: Char = config.as[Option[Char]]("listDelimiter").getOrElse(2)
  val mapFieldDelimiter: Char = config.as[Option[Char]]("mapFieldDelimiter").getOrElse(3)
  val nullValue: String = config.as[Option[String]]("nullValue").getOrElse("")
  val multiLine: Boolean = config.as[Option[Boolean]]("multiLine").getOrElse(false)

  val timeFormatter = DateFormatter(config.as[Option[String]]("timeFormat"))

  lazy val jsonSerDe = JsonSerDe(config)
  lazy val parser = new CSVParser(delimiter, enclosure, escape)

  def convertToDelimited(value: PValue, field: FieldType, level: Int = 2): String = field match {
    case f: RecordField =>
      val map = value match {
        case PMap(m) => m
        case _ => throw new IllegalStateException(s"Can not serialize $value as Record ${f.name}")
      }
      f.fields map (innerField => {
        val res = map.get(innerField.writeName) map (convertToDelimited(_, innerField, level + 1))
        res.getOrElse(nullValue)
      }) mkString getDelimiter(level)
    case f: MapField =>
      val map = value match {
        case PMap(m) => m
        case _ => throw new IllegalStateException(s"Can not serialize $value as Record ${f.name}")
      }
      (f.field map (innerField => {
        map.mapValues(convertToDelimited(_, innerField, level + 2))
      })).getOrElse(map.mapValues(valueToString(_, level + 2))) map {
        case (k, v) => Array(k, v).mkString(getDelimiter(level + 1))
      } mkString getDelimiter(level)
    case f: ListField =>
      val list = value match {
        case PList(l) => l
        case _ => throw new IllegalStateException(s"Can not serialize $value as List ${f.name}")
      }
      f field match {
        case Some(innerField) => list.map(convertToDelimited(_, innerField, level + 1)).mkString(getDelimiter(level))
        case None => list.map(valueToString(_, level + 1)) mkString getDelimiter(level)
      }
    case _ => valueToString(value, level)
  }

  def convertFromDelimited(str: String, field: FieldType, level: Int = 2): Option[PValue] = field match {
    case f: RecordField =>
      val map = str.split(getDelimiter(level)).flatMap(kv=> {
        val parts = kv.split(getDelimiter(level + 1))
        if (parts.size != 2) {
          logger.warn(s"Unexpected delimited map content. Expected [key,value] but get: $kv")
          None
        } else {
          Some(parts(0) -> parts(1))
        }
      }).toMap
      val res = for {
        cf <- f.fields
        cv <- map.get(cf.name)
        pv <- convertFromDelimited(cv, cf,  level + 2)
      } yield cf.name -> pv
      if (res.isEmpty) None else Some(res.toMap)
    case f: MapField =>
      val map = str.split(getDelimiter(level)).flatMap(kv=> {
        val parts = kv.split(getDelimiter(level + 1))
        if (parts.size != 2) {
          logger.warn(s"Unexpected delimited map content. Expected [key,value] but get: $kv")
          None
        } else {
          Some(parts(0) -> parts(1))
        }
      }).toMap
      if (map.isEmpty || f.field.isEmpty) None else {
        val res = map.mapValues(convertFromDelimited(_, f.field.get, level + 2)) collect {
          case (k, Some(v)) => k -> v
        }
        if (res.isEmpty) None else Some(res)
      }
    case f: ListField =>
      val parts = str.split(getDelimiter(level))
      if (parts.isEmpty) None else f.field match {
        case Some(listField) =>
          val res = parts.flatMap(convertFromDelimited(_, listField, level + 1))
          if (res.isEmpty) None else Some(res.toList)
        case _ => Some(parts.map(PValue.apply).toList)
      }
    case v => Some(PString(str))
  }

  def valueToString(value: PValue, level: Int = 2): String = value match {
    case PString(str) => if (str.trim.isEmpty) nullValue else str
    case PInt(num) => num.toString
    case PLong(num) => num.toString
    case PDouble(num) => num.toString
    case PBool(b) => if (b) "true" else "false"
    case PDate(time) => valueToString(timeFormatter.format(time))
    case list: PList => compact(render(jsonSerDe.convertToJson(list)))
    case map: PMap => compact(render(jsonSerDe.convertToJson(map)))
  }

  def getDelimiter(level: Int): String =
    (if (level == 1) {
      delimiter
    } else if (level == 2) {
      listDelimiter
    } else if (level == 3) {
      mapFieldDelimiter
    } else if (level < 25) {
      level.toChar
    } else throw new IllegalStateException("Exceed the maximum level 24 of nesting for csv serializer")).toString

  override def write(value: PValue): Array[Byte] = value match {
    case PMap(map) =>
      try {
        val res = fields.map(f => map.get(f.writeName) match {
          case Some(v) => convertToDelimited(v, f, 2)
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

  def read(value: String): PValue = {
    val csvValues = if (multiLine) parser.parseLineMulti(value.toString) else parser.parseLine(value)
    var i = -1
    val res = fields flatMap (f=> {
      i += 1
      convertFromDelimited(csvValues(i), f) map (v=> f.name -> v)
    })
    res.toMap
  }

  override def read(value: Array[Byte]): PValue = read(value.asStr)
}

case class DelimitedSerDe(config: Config) extends DelimitedSerDeTrait

case class CsvSerDe(config: Config) extends DelimitedSerDeTrait {
  override val delimiter = ','
  override val listDelimiter = '|'
  override val mapFieldDelimiter = ':'
}

case class TsvSerDe(config: Config) extends DelimitedSerDeTrait {
  override val delimiter = '\t'
  override val listDelimiter = '|'
  override val mapFieldDelimiter = ':'
}

case class HiveTsvSerDe(config: Config) extends DelimitedSerDeTrait {
  override val delimiter = '\t'
  override val listDelimiter = '|'
  override val mapFieldDelimiter = ':'
  override val enclosure = CSVWriter.NO_ESCAPE_CHARACTER
}