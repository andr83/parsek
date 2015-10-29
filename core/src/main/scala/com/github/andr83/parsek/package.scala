package com.github.andr83


import com.typesafe.config.{Config, ConfigObject, ConfigValue}
import org.joda.time.DateTime

/**
 * @author @andr83
 */
package object parsek {
  implicit def strToPValue(value: String): PString = PString(value)

  implicit def intToPValue(value: Int): PInt = PInt(value)

  implicit def longToPValue(value: Long): PLong = PLong(value)

  implicit def timeToPValue(value: DateTime): PTime = PTime(value)

  implicit def floatToPValue(value: Float): PDouble = PDouble(value)

  implicit def doubleToPValue(value: Double): PDouble = PDouble(value)

  implicit def booleanToPValue(value: Boolean): PBool = PBool(value)

  implicit def mapToPValue(value: Map[String, PValue]): PMap = PMap(value)

  implicit def listToPValue(value: List[PValue]): PList = PList(value)

  implicit class RichConfig(val underlying: Config) extends AnyVal {

    import scala.collection.JavaConversions._

    def getBooleanOpt(path: String): Option[Boolean] = if (underlying.hasPath(path)) {
      Some(underlying.getBoolean(path))
    } else {
      None
    }

    def getStringOpt(path: String): Option[String] = if (underlying.hasPath(path)) {
      Some(underlying.getString(path))
    } else {
      None
    }

    def getStringListOpt(path: String): Option[List[String]] = if (underlying.hasPath(path)) {
      Some(underlying.getStringList(path).toList)
    } else {
      None
    }

    def getConfigOpt(path: String): Option[Config] = if (underlying.hasPath(path)) {
      Some(underlying.getConfig(path))
    } else {
      None
    }

    def getConfigListOpt(path: String): Option[List[Config]] = if (underlying.hasPath(path)) {
      Some(underlying.getConfigList(path).toList)
    } else {
      None
    }

    def getObjectListOpt(path: String): Option[List[ConfigObject]] = if (underlying.hasPath(path)) {
      underlying getValue path match {
        case obj: ConfigObject => Some(List(obj))
        case list: List[_] => Some(list.map(_.asInstanceOf[ConfigObject]))
        case value => throw new IllegalStateException(s"Configuration error in path $path. Expected object actual $value")
      }
    } else {
      None
    }

    def getMapOpt(path: String): Option[Map[String, ConfigValue]] = if (underlying.hasPath(path)) {
      val c = underlying.getConfig(path)
      Some(c.entrySet().map {
        case entry: java.util.Map.Entry[String, ConfigValue] => entry.getKey -> entry.getValue
      }.toMap)
    } else {
      None
    }
  }

}
