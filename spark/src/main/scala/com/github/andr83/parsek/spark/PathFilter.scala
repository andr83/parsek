package com.github.andr83.parsek.spark

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.DateFormatter
import com.github.andr83.parsek.util.RegexUtils
import com.github.nscala_time.time.Imports._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.fs.Path
import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
  * Filters implementations to filter input path
  *
  * @author andr83
  */
object PathFilter {
  type PathFilter = Path => Boolean

  val timeParser = new DateTimeFormatterBuilder()
    .append(null, Array(
      DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss").getParser,
      DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm").getParser,
      DateTimeFormat.forPattern("yyyy-MM-dd-HH").getParser,
      DateTimeFormat.forPattern("yyyy-MM-dd").getParser
    )).toFormatter

  /**
    * Filter path by time getting from file name.
    * Time setting in from - to range in format accepted by timeParser
    *
    * @param from from time limit. Inclusive.
    * @param to to time limit. Exclusive.
    * @param format date format string to parse time from file name
    * @param pattern regex pattern to get time string. Must contain "time" group.
    */
  case class TimeFilter(
    from: Long,
    to: Long,
    format: DateFormatter,
    pattern: Regex) extends PathFilter with LazyLogging {
    def this(config: Config) = this(
      from = config.as[Option[String]]("from").map(timeParser.parseDateTime(_).getMillis).getOrElse(0),
      to = config.as[Option[String]]("to").map(timeParser.parseDateTime(_).getMillis).getOrElse(Long.MaxValue),
      format = DateFormatter(Some(config.as[String]("format")), config.as[Option[DateTimeZone]]("timeZone")),
      pattern = config.as[String]("pattern").r
    )

    val dateGroupIdx = RegexUtils.getNamedGroups(pattern).getOrElse("time",
      throw new IllegalStateException("TimeFilter pattern must contain \"time\" group"))

    def apply(path: Path): Boolean = {
      val res = Try(for (m <- pattern.findFirstMatchIn(path.getName)) yield {
        val dt = format.parse(m.group(dateGroupIdx)).value.getMillis
        dt >= from && dt < to
      }) match {
        case Success(v) => v
        case Failure(error) =>
          logger.warn(error.toString)
          None
      }
      res.getOrElse(false)
    }
  }

  /**
    * Filter file name by regex expression.
    *
    * @param pattern regex expression to match file name
    */
  case class RegexFilter(pattern: Regex) extends PathFilter with LazyLogging {
    def this(config: Config) = this(config.as[String]("pattern").r)

    def apply(path: Path): Boolean = Try(pattern.pattern.matcher(path.toString).matches()) match {
      case Success(v) => v
      case Failure(error) =>
        logger.warn(error.toString)
        false
    }
  }

  def apply(config: Config): PathFilter = {
    val filterType = config.as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("Path filter config must have type property"))
    val className = if (filterType.contains(".")) filterType
    else
      getClass.getName + filterType.head.toUpper + filterType.substring(1) + "Filter"
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[PathFilter]
  }
}
