package com.github.andr83.parsek.pipe.parser

import java.lang.reflect.Method

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.TransformPipe
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import scala.collection.JavaConversions._
import scala.util.matching.Regex

/**
 * @author andr83
 */
case class RegexParser(config: Config) extends TransformPipe(config) {
  val regex = config.as[String]("pattern").r
  val namedGroups = RegexParser.getNamedGroups(regex)

  override def transformString(str: String)(implicit context: Context): Option[PValue] = for (
    m <- regex.findFirstMatchIn(str)
    if m.groupCount > 0
  ) yield {
      val map = for (
        (name, idx) <- namedGroups
      ) yield name -> PString(m.group(idx))
      PMap(map.toMap)
    }
}

object RegexParser {
  def getNamedGroups(regex: Regex): Map[String, Int] = {
    try {
      val namedGroupsMethod: Method = regex.pattern.getClass.getDeclaredMethod("namedGroups")
      namedGroupsMethod.setAccessible(true)
      namedGroupsMethod.invoke(regex.pattern).asInstanceOf[java.util.Map[String, Int]].toMap[String, Int]
    } catch {
      case _: Throwable => Map.empty[String, Int]
    }
  }

  def apply(): RegexParser = RegexParser(ConfigFactory.empty())
}