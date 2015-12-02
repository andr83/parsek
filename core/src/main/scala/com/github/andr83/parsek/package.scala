package com.github.andr83


import java.util.Locale

import com.github.andr83.parsek.meta._
import com.github.nscala_time.time.Imports._
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.matching.Regex

/**
 * @author @andr83
 */
package object parsek {
  implicit def strToPValue(value: String): PString = PString(value)

  implicit def intToPValue(value: Int): PInt = PInt(value)

  implicit def longToPValue(value: Long): PLong = PLong(value)

  implicit def timeToPValue(value: DateTime): PDate = PDate(value)

  implicit def floatToPValue(value: Float): PDouble = PDouble(value)

  implicit def doubleToPValue(value: Double): PDouble = PDouble(value)

  implicit def booleanToPValue(value: Boolean): PBool = PBool(value)

  implicit def mapToPValue(value: Map[String, PValue]): PMap = PMap(value)

  implicit def listToPValue(value: List[PValue]): PList = PList(value)

  implicit def mapToConfig(map: Map[String, AnyRef]): Config = ConfigFactory.parseMap(map)

  implicit val fieldConfigReader: ValueReader[FieldType] = ValueReader.relative(config => {
    Field.apply(config)
  })

  implicit val dateConfigReader: ValueReader[DateField] = ValueReader.relative(config => {
    val timeZone = config.as[Option[DateTimeZone]]("timeZone")
    val pattern = (config.as[Option[String]]("format")
      .map(fmt=> DateTimeFormat.forPattern(fmt)) getOrElse ISODateTimeFormat.dateTime()).withLocale(Locale.ENGLISH)
    DateField(
      name = config.as[String]("name"),
      pattern = timeZone map (tz => pattern.withZone(tz)) getOrElse pattern,
      toTimeZone = config.as[Option[DateTimeZone]]("toTimeZone")
    )
  })

  implicit val charReader: ValueReader[Char] = new ValueReader[Char] {
    def read(config: Config, path: String): Char = config.getString(path).head
  }

  implicit val regexReader: ValueReader[Regex] = new ValueReader[Regex] {
    def read(config: Config, path: String): Regex = config.getString(path).r
  }

  implicit val stringCaseReader: ValueReader[StringCase] = new ValueReader[StringCase] {
    def read(config: Config, path: String): StringCase = config.getString(path).toLowerCase match {
      case "lower" => LowerCase
      case "upper" => UpperCase
      case _ =>
        throw new ConfigException.BadValue(config.origin(), path, "String case can be only \"upper\" or \"lower\"")
    }
  }

  implicit val timeZoneReader: ValueReader[DateTimeZone] = new ValueReader[DateTimeZone] {
    def read(config: Config, path: String): DateTimeZone = DateTimeZone.forID(config.getString(path))
  }

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
