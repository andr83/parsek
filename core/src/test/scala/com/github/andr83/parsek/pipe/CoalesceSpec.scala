package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author andr83
  */
class CoalesceSpec extends FlatSpec with Matchers {
  implicit val context = new PipeContext()

  it should "return first non empty value and update with it first field" in {
    val pipe = Coalesce(fields = Seq("f1".asFieldPath, "f2".asFieldPath))
    val value = PMap("f2" -> PString("v2"))

    val result = pipe.run(value)

    result shouldBe Some(PMap(
      "f1" -> "v2",
      "f2" -> "v2"
    ))
  }
}
