package com.github.andr83.parsek

import com.github.nscala_time.time.Imports._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

/**
 * @author andr83
 */
trait DateFormatter {
  def format(date: DateTime): PValue
}

object TimestampFormatter extends DateFormatter {
  override def format(date: DateTime): PLong = PLong(date.getMillis)
}

class JodaDateFormatter(formatter: DateTimeFormatter) extends DateFormatter {
  override def format(date: DateTime): PString = date.toString(formatter)
}

object DateFormatter {
  def apply(pattern: Option[String]): DateFormatter = pattern match {
    case Some("timestamp") => TimestampFormatter
    case Some(str) => new JodaDateFormatter(DateTimeFormat.forPattern(str))
    case None => new JodaDateFormatter(ISODateTimeFormat.dateTime())
  }
}