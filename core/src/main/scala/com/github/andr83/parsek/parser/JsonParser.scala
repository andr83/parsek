package com.github.andr83.parsek.parser

import org.json4s._
import org.json4s.jackson.JsonMethods.{parse => jsonParse}

import com.github.andr83.parsek._

/**
 * @author andr83
 */
class JsonParser(implicit override val context: PipeContext) extends StringParser {

  override def parseString(source: String): PValue = {
    val json = jsonParse(source)
    mapJson(json)
  }

  def mapJson(json: JValue): PValue = json match {
    case JString(s) => PString(s)
    case JDouble(num) => PDouble(num)
    case JDecimal(num) => PDouble(num.toDouble)
    case JInt(num) => PInt(num.toInt)
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
  def apply()(implicit context: PipeContext): JsonParser = new JsonParser()
}
