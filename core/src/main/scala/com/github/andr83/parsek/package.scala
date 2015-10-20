package com.github.andr83

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
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
  }

}
