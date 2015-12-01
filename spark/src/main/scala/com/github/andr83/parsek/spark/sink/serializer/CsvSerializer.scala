package com.github.andr83.parsek.spark.sink.serializer

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.sink.Serializer
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}

import scala.collection.JavaConversions._
import scala.language.postfixOps

/**
 * @author andr83
 */
class CsvSerializer(config: Config) extends Serializer(config) {
  var fields = config.getStringList("fields").toList
  val delimiter = config.as[Option[String]]("delimiter").getOrElse(",")
  val enclosure = config.as[Option[String]]("enclosure").getOrElse("\"")
  val listDelimiter = config.as[Option[String]]("listDelimiter").getOrElse("|")
  val mapFieldDelimiter = config.as[Option[String]]("mapFieldDelimiter").getOrElse(":")

  val timeFormatter = config.as[Option[String]]("timeFormat")
    .map(DateTimeFormat.forPattern)
    .getOrElse(ISODateTimeFormat.dateTime())

  override def write(value: PValue): Array[Byte] = value match {
    case PMap(map) =>
      val res: Iterable[Option[String]] = for {
        field <- fields
      } yield map.getValue(field).map(valueToString)
      res map {
        case Some(str) => quoteAndEscape(str)
        case None => ""
      } mkString delimiter map (_.toByte) toArray
    case _ => Array.empty[Byte]
  }

  def valueToString(value: PValue): String = value match {
    case PString(str) => str
    case PInt(num) => num.toString
    case PLong(num) => num.toString
    case PDouble(num) => num.toString
    case PBool(b) => if (b) "true" else "false"
    case PDate(time) => time.toString(timeFormatter)
    case PList(list) => list.map(valueToString) mkString listDelimiter
    case PMap(map) => map mapValues valueToString map {
      case (k, v) => k + mapFieldDelimiter + v
    } mkString listDelimiter
  }

  def quoteAndEscape(str: String): String = {
    val res = if (str.contains(enclosure)) {
      str.replaceAll(enclosure, enclosure + enclosure)
    } else str
    if (str.contains(delimiter)) {
      enclosure + res + enclosure
    } else res
  }
}
