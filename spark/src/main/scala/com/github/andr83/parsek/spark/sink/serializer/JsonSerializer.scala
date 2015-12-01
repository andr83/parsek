package com.github.andr83.parsek.spark.sink.serializer

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.sink.Serializer
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * @author andr83
 */
class JsonSerializer(config: Config) extends Serializer(config) {
  val fields = config.as[Option[List[String]]]("fields")
  val timeFormatter = config.as[Option[String]]("timeFormat")
    .map(DateTimeFormat.forPattern)
    .getOrElse(ISODateTimeFormat.dateTime())

  override def write(value: PValue): Array[Byte] = fields match {
    case Some(fs) =>
      val map = value.asInstanceOf[PMap].value
      compact(render(convertToJson(map.filterKeys(fs.contains)))).asBytes
    case None => compact(render(convertToJson(value))).asBytes
  }

  def convertToJson(value: PValue): JValue = value match {
    case PString(str) => JString(str)
    case PInt(num) => JInt(num)
    case PLong(num) => JLong(num)
    case PDouble(num) => JDouble(num)
    case PBool(num) => JBool(num)
    case PDate(date) => JString(date.toString(timeFormatter))
    case PList(list) => JArray(list.map(convertToJson))
    case PMap(map) => JObject(map.mapValues(convertToJson).toList)
  }
}
