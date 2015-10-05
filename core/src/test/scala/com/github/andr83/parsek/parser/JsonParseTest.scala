package com.github.andr83.parsek.parser

import com.github.andr83.parsek._
import org.scalatest.{Inside, Matchers, FlatSpec}

/**
 * @author andr83
 */
class JsonParseTest extends FlatSpec with Matchers with Inside {
  implicit val context = new PipeContext()

  "Json string" should "be parsed to PMap value" in {
    val json = PString(
      """
        |{
        | "fieldStr": "Some string",
        | "fieldInt": 10,
        | "fieldDouble": 1.5,
        | "fieldArray": [2,3,4],
        | "fieldMap": {
        |   "innerField": "Other value"
        | }
        |}
      """.stripMargin)
    val parser = JsonParser()
    val result = parser.parse(json)

    inside(result) {
      case PMap(map) =>
        map should contain allOf (
          "fieldStr" -> PString("Some string"),
          "fieldInt" -> PInt(10),
          "fieldDouble" -> PDouble(1.5),
          "fieldArray" -> PList(PInt(2) :: PInt(3) :: PInt(4) :: Nil),
          "fieldMap" -> PValue(Map(
            "innerField" -> "Other value"
          )))
    }
  }
}
