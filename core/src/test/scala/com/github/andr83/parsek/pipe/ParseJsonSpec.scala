package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import org.scalatest.{FlatSpec, Inside, Matchers}

/**
 * @author andr83
 */
class ParseJsonSpec extends FlatSpec with Matchers with Inside {
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
    val parser = ParseJsonPipe()
    val result = parser.run(json)

    result shouldBe Some(PMap(
      "fieldStr" -> PString("Some string"),
      "fieldInt" -> PLong(10),
      "fieldDouble" -> PDouble(1.5),
      "fieldArray" -> PList(PLong(2) :: PLong(3) :: PLong(4) :: Nil),
      "fieldMap" -> PMap(
        "innerField" -> PString("Other value")
      )
    ))
  }
}
