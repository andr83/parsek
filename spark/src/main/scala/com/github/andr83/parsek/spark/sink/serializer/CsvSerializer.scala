package com.github.andr83.parsek.spark.sink.serializer

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.sink.Serializer
import com.typesafe.config.Config
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConversions._

/**
 * @author andr83
 */
class CsvSerializer(config:Config) extends Serializer(config) {
  var fields = config.getStringList("fields").toList
  val delimeter = config.getStringOpt("delimiter").getOrElse(",")
  val enclosure = config.getStringOpt("enclosure").getOrElse("\"")
  val listDelimiter = config.getStringOpt("listDelimiter").getOrElse("|")
  val mapFieldDelimiter = config.getStringOpt("mapFieldDelimiter").getOrElse(":")

  val timeFormatter = {
    val format = config.getStringOpt("timeFormat").getOrElse("yyyy-MM-dd HH:mm:ss")
    DateTimeFormat.forPattern(format)
  }

  override def write(value: PValue): Array[Byte] = value match {
    case PMap(map) =>
      val res: Iterable[Option[String]] = for {
        field <- fields
      } yield map.get(field).map(valueToString)
      res map {
        case Some(str) => quoteAndEscape(str)
        case None => ""
      } mkString delimeter map(_.toByte) toArray
    case _ => Array.empty[Byte]
  }

  def valueToString(value: PValue): String = value match {
    case PString(str) => str
    case PInt(num) => num.toString
    case PLong(num) => num.toString
    case PDouble(num) => num.toString
    case PBool(b) => if (b) "1" else "0"
    case PTime(time) => time.toString(timeFormatter)
    case PList(list) => list.map(valueToString) mkString listDelimiter
    case PMap(map) => map mapValues valueToString map {
      case (k,v) => k + mapFieldDelimiter + v
    } mkString listDelimiter
  }

  def quoteAndEscape(str: String): String = {
    val res = if (str.contains(enclosure)) {
      str.replaceAll(enclosure, enclosure + enclosure)
    } else str
    if(str.contains(delimeter)) {
      enclosure + res + enclosure
    } else res
  }
}
