package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.{Config, ConfigFactory}
import org.json4s._
import org.json4s.jackson.JsonMethods.{parse => jsonParse}

/**
 * @author andr83
 */
case class JsonParser(config: Config) extends TransformPipe(config) {

  def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    val json = jsonParse(str)
    Some(mapJson(json))
  }

  def mapJson(json: JValue): PValue = json match {
    case JString(s) => PString(s)
    case JDouble(num) => PDouble(num)
    case JDecimal(num) => PDouble(num.toDouble)
    case JInt(num) => PLong(num.toLong)
    case JLong(num) => PLong(num)
    case JBool(b) => PBool(b)
    case JObject(fields) =>
      PMap(fields.filter {
        case (_, JNothing) => false
        case (_, JNull) => false
        case _ => true
      }.toMap.mapValues(mapJson))
    case JArray(arr) =>
      PList(arr.filter {
        case JNothing => false
        case JNull => false
        case _ => true
      }.map(mapJson))
    case _ => throw new IllegalStateException(s"Unexpected value parsing json $json")
  }
}

object JsonParser {
  def apply(): JsonParser = JsonParser(ConfigFactory.empty())
}