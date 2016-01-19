package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author andr83
  */
class ExtractSpec extends FlatSpec with Matchers {
  implicit val context = new PipeContext()

  "Extract pipe " should " move values from inner PMap to other PMap " in {
    val pipe = ExtractPipe(from="logs.data", to="logs.data2")
    val value = PMap("logs" ->
      PMap("data" ->
        PMap(
          "k1" -> PString("v1"),
          "k2" -> PString("v2")
        )
      )
    )

    val result = pipe.run(value)

    result shouldBe Some(PMap("logs" ->
      PMap("data2" ->
        PMap(
          "k1" -> PString("v1"),
          "k2" -> PString("v2")
        )
      )
    ))
  }

  "Extract pipe " should " move values from inner PMap to root" in {
    val pipe = ExtractPipe(from="logs.data")
    val value = PMap("logs" ->
      PMap("data" ->
        PMap(
          "k1" -> PString("v1"),
          "k2" -> PString("v2")
        )
      )
    )

    val result = pipe.run(value)

    result shouldBe Some(PMap(
      "k1" -> PString("v1"),
      "k2" -> PString("v2")
    ))
  }

  "Extract pipe " should "throw IllegalStateException on invalid path" in {
    val pipe = ExtractPipe(from="logs.data")
    val value = PMap("logs" ->
      PMap("data" -> PString("data string"))
    )

    a[IllegalStateException] shouldBe thrownBy {
      pipe.run(value)
    }
  }
}
