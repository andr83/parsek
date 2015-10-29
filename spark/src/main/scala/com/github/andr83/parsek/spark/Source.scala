package com.github.andr83.parsek.spark

import com.github.andr83.parsek._
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD

/**
 * @author andr83
 */
abstract class Source(config: Config) {
  def apply(job: SparkJob): RDD[String]
}

object Source {
  def apply(config: Config): Source = {
    val sourceType = config.getStringOpt("type")
      .getOrElse(throw new IllegalStateException("Source config should have type property"))
    val className = if (sourceType.contains(".")) sourceType else
      "com.github.andr83.parsek.spark.source." + sourceType.head.toUpper + sourceType.substring(1)
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[Source]
  }
}
