package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author
  */
class FlattenSpec extends FlatSpec with Matchers {
  implicit val context = new PipeContext()

  "Embedded lists " should "be flatten only in field level" in {
    val pipe = FlattenPipe(field = "logs.data".asFieldPath, recursively = false)

    val value = PMap("logs" ->
      PMap("data" -> PList(List(
        PList(List(
          PMap("k1" -> PString("v1")),
          PMap("k2" -> PString("v2"))
        )),
        PList(List(
          PMap("k1" -> PString("v11")),
          PMap("k3" -> PString("v3"))
        ))
      )))
    )

    val result = pipe.run(value)

    result should contain (PMap("logs" ->
      PMap("data" ->
        PList(List(
          PMap("k1" -> PString("v1")),
          PMap("k2" -> PString("v2")),
          PMap("k1" -> PString("v11")),
          PMap("k3" -> PString("v3"))
        ))
      )
    ))
  }

  "Embedded lists " should "be flatten recursively" in {
    val pipe = FlattenPipe("logs.data".asFieldPath)

    val value = PMap("logs" ->
      PMap("data" ->
        PList(List(
          PMap("k1" -> PString("v1")),
          PMap("k2" -> PString("v2"))
        ))
      )
    )

    val result = pipe.run(value)

    result should contain (PList(List(
      PMap("k1" -> PString("v1")),
      PMap("k2" -> PString("v2"))
    )))
  }
}
