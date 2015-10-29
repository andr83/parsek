package com.github.andr83.parsek.spark.source

import com.github.andr83.parsek.spark.{Source, SparkJob}
import com.github.andr83.parsek.{PString, PValue}
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD

/**
 * @author
 */
class TextFileSource(config: Config) extends Source(config) {
  val path = config.getAnyRef("path") match {
    case list: List[_] => list.map(_.toString)
    case value => List(value.toString)
  }

  override def apply(job: SparkJob): RDD[PValue] = {

    job.sc.textFile(path.mkString(",")).map(PString)
  }
}
