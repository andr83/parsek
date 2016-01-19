package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import org.scalatest._

/**
 * @author andr83
 */
class ParseRegexSpec extends FlatSpec with Matchers with Inside {
  implicit val context = new PipeContext()

  val line = "2014-11-11 06:00:00, [191.247.226.134],[&imei=8a7edf8a47c923f21096fe51660d341b&os=Android&version=16]"

  "Regex parser" should "return PMap" in {
    val parser = new ParseRegexPipe("(?<time>[\\d\\s-:]+),\\s+\\[(?<ip>[\\d\\.]+)\\].+\\[(?<q>.+)\\].*".r)
    val result = parser.transformString(line)

    result shouldBe Some(PMap(
      "time" -> "2014-11-11 06:00:00",
      "ip" -> "191.247.226.134",
      "q" -> "&imei=8a7edf8a47c923f21096fe51660d341b&os=Android&version=16"
    ))
  }
}
