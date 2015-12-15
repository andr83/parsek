package com.github.andr83.parsek.spark.sink

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.Sink
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

/**
  * Simple sink which just output to log count of records in rdd
  *
  * @param level log level for output. Default is INFO
  *
  * @author andr83
 */
class LogCountSink(level: Level = Level.INFO) extends Sink {

  def this(config: Config) = this(Level.toLevel(config.as[Option[String]]("level").getOrElse("info").toUpperCase))

  override def sink(rdd: RDD[PValue]): Unit = {
    val count = rdd.count().toString
    level match {
      case Level.INFO => logger.info(count)
      case Level.WARN => logger.warn(count)
      case Level.ERROR => logger.error(count)
      case _ => logger.debug(count)
    }
  }
}
