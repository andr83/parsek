package com.github.andr83.parsek.serde

import com.github.andr83.parsek.{PipeContext, _}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author andr83 
  *         created on 01.07.16
  */
class JsonSerDeSpec extends FlatSpec with Matchers {
  implicit val context = new PipeContext()

  "Json string" should "parse to PValue and serialize back" in {
    val str =
      """{"data":{"currency":"Kč"},"sessionId":"af652b63d57c6cb508fd9176ffkf65e48c78ef38","created":1467158651342}"""

    val serde = JsonSerDe()
    val pv = serde.read(str.asBytes)
    val json = serde.write(pv).asStr
    assert(str == json)
  }
}
