package com.github.andr83.parsek.formatter

import java.util.Locale

import com.github.andr83.parsek._
import com.github.nscala_time.time.Imports._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
 * @author andr83
 */
trait DateFormatter extends Serializable {
  def format(date: DateTime): PValue
  def parse(value: PValue): PDate
}

object TimestampFormatter extends DateFormatter {
  override def format(date: DateTime): PLong = PLong(date.getMillis)

  override def parse(value: PValue): PDate = value match {
    case v: PDate => v
    case PInt(num) => new DateTime(num * 1000L)
    case PLong(num) => new DateTime(if (num >= 100000000000L) num else num * 1000)
    case PString(str) =>
      val num = str.toLong
      new DateTime(if (num >= 100000000000L) num else num * 1000)
    case _ => throw new IllegalStateException(s"Can not parse value $value as timestamp")
  }
}

class JodaDateFormatter(formatterFactory: () => DateTimeFormatter) extends DateFormatter {
  val formatter = formatterFactory()
  override def format(date: DateTime): PString = date.toString(formatter)

  override def parse(value: PValue): PDate = value match {
    case v: PDate => v
    case PString(str) => formatter.parseDateTime(str)
    case _ => throw new IllegalStateException(s"Can not parse value $value as date")
  }
}

object DateFormatter {
  val DefaultDatePattern = "yyyy-MM-dd HH:mm:ss"

  def apply(pattern: Option[String], timeZone: Option[DateTimeZone] = None): DateFormatter = pattern match {
    case Some("timestamp") => TimestampFormatter
    case Some(str) => new JodaDateFormatter(() => DateTimeFormat.forPattern(str).withLocale(Locale.ENGLISH))
    case None => new JodaDateFormatter(() => DateTimeFormat.forPattern(DefaultDatePattern).withLocale(Locale.ENGLISH))
  }
}