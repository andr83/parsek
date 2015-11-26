package com.github.andr83


import org.joda.time.DateTime

import scala.language.implicitConversions

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

  implicit class RichString(val str: String) extends AnyVal {
    def asBytes: Array[Byte] = str.map(_.toByte).toArray
  }

  implicit class RichByteArray(val arr: Array[Byte]) extends AnyVal {
    def asStr: String = new String(arr.map(_.toChar))
  }

  case class IntCounter(var count: Int = 0) extends Serializable {
    def +=(inc: Int): IntCounter = {
      count += inc
      this
    }
  }
}
