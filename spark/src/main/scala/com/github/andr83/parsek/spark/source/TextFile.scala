package com.github.andr83.parsek.spark.source

import com.github.andr83.parsek.spark.{SparkJob, Source}
import com.typesafe.config.{ConfigList, Config}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/**
 * @author
 */
class TextFile(config: Config) extends Source(config) {
  override def apply(job: SparkJob): RDD[String] = {
    val path = config.getAnyRef("path") match {
      case list: List[_] => list.map(_.toString)
      case value => List(value.toString)
    }
    job.sc.textFile(path.mkString(","))
  }
}
