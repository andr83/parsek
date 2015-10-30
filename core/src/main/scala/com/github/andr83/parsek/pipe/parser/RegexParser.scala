package com.github.andr83.parsek.pipe.parser

import java.lang.reflect.Method

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.TransformPipe
import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.util.matching.Regex

/**
 * @author andr83
 */
case class RegexParser(config: Config) extends TransformPipe(config) {
  val regex = config.getStringReq("pattern").r
  val namedGroups = RegexParser.getNamedGroups(regex)

  override def transformString(raw: String): Option[PValue] = for (
    m <- regex.findFirstMatchIn(raw)
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
}