package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author andr83
  */
class MergeSpec extends FlatSpec with Matchers {
  implicit val context = new PipeContext()

  "Multiple PMap values" should "be merged to one PMap and store on parent level" in {
    val pipe = Merge(fields = Seq("data.f1".asFieldPath, "data.f2".asFieldPath))

    val value =
      PMap("data" ->
        PMap(
          "f1" -> PMap(
            "k1" -> PString("v1"),
            "k2" -> PString("v2")
          ),
          "f2" -> PMap(
            "k1" -> PString("v11"),
            "k3" -> PString("v3")
          )
        )
      )

    val result = pipe.run(value)
    result should contain (PMap("data" ->
        PMap(
          "k1" -> PString("v11"),
          "k2" -> PString("v2"),
          "k3" -> PString("v3")
        )
      )
    )
  }

  "Multiple PMap values" should "be merged to one PMap and store as first key" in {
    val pipe = Merge(fields = Seq("data.f1".asFieldPath, "data.f2".asFieldPath), as = Some("f1"))

    val value =
      PMap("data" ->
        PMap(
          "f1" -> PMap(
            "k1" -> PString("v1"),
            "k2" -> PString("v2")
          ),
          "f2" -> PMap(
            "k1" -> PString("v11"),
            "k3" -> PString("v3")
          )
        )
      )

    val result = pipe.run(value)
    result should contain (PMap("data" ->
      PMap(
        "f1" -> PMap(
          "k1" -> PString("v11"),
          "k2" -> PString("v2"),
          "k3" -> PString("v3")
        ),
        "f2" -> PMap(
          "k1" -> PString("v11"),
          "k3" -> PString("v3")
        )
      )
    ))
  }

  "Merge " must "throw IllegalStateException on getting fields on different levels" in {
    a[IllegalStateException] shouldBe thrownBy {
      Merge(fields = Seq("f1".asFieldPath, "data.f2".asFieldPath))
    }
  }
}
