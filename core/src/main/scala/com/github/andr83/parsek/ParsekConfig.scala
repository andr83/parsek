package com.github.andr83.parsek

import com.github.andr83.parsek.meta._
import com.github.nscala_time.time.Imports._
import com.typesafe.config.{Config, ConfigException}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

import scala.language.implicitConversions
import scala.util.matching.Regex

/**
 * @author andr83
 */
object ParsekConfig {
  implicit val fieldConfigReader: ValueReader[FieldType] = ValueReader.relative(config => {
    Field.apply(config)
  })

  implicit val dateConfigReader: ValueReader[DateField] = ValueReader.relative(config => {
    val timeZone = config.as[Option[DateTimeZone]]("timeZone")
    val formatter = DateFormatter(config.as[Option[String]]("format"), timeZone)
    DateField(
      name = config.as[String]("name"),
      pattern = formatter,
      toTimeZone = config.as[Option[DateTimeZone]]("toTimeZone")
    )
  })

  implicit val pipeConfigReader: ValueReader[Pipe] = ValueReader.relative(Pipe.apply)

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
}
