package com.github.andr83.parsek.util

import java.lang.reflect.Method

import scala.collection.JavaConversions._
import scala.util.matching.Regex

/**
  * @author andr83
  */
object RegexUtils {

  /**
    * Return all name groups from regex
    * @param regex
    * @return
    */
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
