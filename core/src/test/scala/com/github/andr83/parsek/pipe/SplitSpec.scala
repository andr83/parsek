package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import org.scalatest._

/**
  * @author andr83
  */
class SplitSpec extends FlatSpec with Matchers with Inside {
  implicit val context = new PipeContext()

  "String value" should "be split to parts by separator" in {
    val pipe = SplitPipe(";".r)
    val result = pipe.transformString("a;b;c;d")

    result shouldBe Some(PList.create("a", "b", "c", "d"))
  }

  "Additional values" should "be appended to the beginning and end of split parts" in {
    val pipe = SplitPipe("\\}\\{".r, appendToBeginning = "{", appendToEnd = "}")
    val result = pipe.transformString("{a:b}{c:d}")

    result shouldBe Some(PList.create("{a:b}", "{c:d}"))
  }
}
