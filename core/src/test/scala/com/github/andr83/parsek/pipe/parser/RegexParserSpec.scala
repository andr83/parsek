package com.github.andr83.parsek.pipe.parser

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.ParseRegex
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.collection.JavaConversions._

/**
 * @author andr83
 */
class RegexParserSpec extends FlatSpec with Matchers with Inside {
  implicit val context = new PipeContext()

  val line = "2014-11-11 06:00:00, [191.247.226.134],[&imei=8a7edf8a47c923f21096fe51660d341b&os=Android&version=16]"

  "Regex parser" should "return PMap" in {
    val config = ConfigFactory.parseMap(Map(
      "pattern" -> "(?<time>[\\d\\s-:]+),\\s+\\[(?<ip>[\\d\\.]+)\\].+\\[(?<q>.+)\\].*"
    ))
    val parser = new ParseRegex(config)
    val result = parser.transformString(line)

    result shouldBe Some(PMap(Map(
      "time" -> "2014-11-11 06:00:00",
      "ip" -> "191.247.226.134",
      "q" -> "&imei=8a7edf8a47c923f21096fe51660d341b&os=Android&version=16"
    )))
  }
}
