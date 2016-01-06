package com.github.andr83.parsek.serde

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse => jsonParse, render}

/**
 * @author andr83
 */
case class JsonSerDe(config: Config) extends SerDe {
  val fields = config.as[Option[List[String]]]("fields")
  val timeFormatter = DateFormatter(config.as[Option[String]]("timeFormat"))

  override def write(value: PValue): Array[Byte] = fields match {
    case Some(fs) =>
      val map = value.asInstanceOf[PMap].value
      compact(render(convertToJson(map.filterKeys(fs.contains)))).asBytes
    case None => compact(render(convertToJson(value))).asBytes
  }

  override def read(value: Array[Byte]): PValue = {
    val json = jsonParse(value.asStr)
    convertFromJson(json)
  }

  def convertToJson(value: PValue): JValue = value match {
    case PString(str) => JString(str)
    case PInt(num) => JInt(num)
    case PLong(num) => JInt(num)//JLong(num)
    case PDouble(num) => JDouble(num)
    case PBool(num) => JBool(num)
    case PDate(date) => convertToJson(timeFormatter.format(date))
    case PList(list) => JArray(list.map(convertToJson))
    case PMap(map) => JObject(map.mapValues(convertToJson).toList)
  }

  def convertFromJson(json: JValue): PValue = json match {
    case JString(s) => PString(s)
    case JDouble(num) => PDouble(num)
    case JDecimal(num) => PDouble(num.toDouble)
    case JInt(num) => PLong(num.toLong)
//    case JLong(num) => PLong(num)
    case JBool(b) => PBool(b)
    case JObject(obj) =>
      PMap(obj.filter {
        case (_, JNothing) => false
        case (_, JNull) => false
        case _ => true
      }.toMap.mapValues(convertFromJson))
    case JArray(arr) =>
      PList(arr.filter {
        case JNothing => false
        case JNull => false
        case _ => true
      }.map(convertFromJson))
    case _ => throw new IllegalStateException(s"Unexpected value parsing json $json")
  }
}
