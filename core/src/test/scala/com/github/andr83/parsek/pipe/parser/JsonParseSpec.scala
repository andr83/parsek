package com.github.andr83.parsek.pipe.parser

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.ParseJson
import org.scalatest.{FlatSpec, Inside, Matchers}

/**
 * @author andr83
 */
class JsonParseSpec extends FlatSpec with Matchers with Inside {
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
    val parser = ParseJson()
    val result = parser.run(json)

    result shouldBe Some(PMap(Map(
      "fieldStr" -> PString("Some string"),
      "fieldInt" -> PInt(10),
      "fieldDouble" -> PDouble(1.5),
      "fieldArray" -> PList(PInt(2) :: PInt(3) :: PInt(4) :: Nil),
      "fieldMap" -> PValue(Map(
        "innerField" -> "Other value"
      )))
    ))
  }
}
