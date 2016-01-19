package com.github.andr83.parsek.spark.source

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.SparkJob
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD

/**
  * Source is an abstract class to create RDD from different sources.
  *
  * @author andr83
  */
abstract class Source extends LazyLogging {
  def apply(job: SparkJob): RDD[PValue]
}

object Source {
  def apply(config: Config): Source = {
    val sourceType = config.as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("Source config should have type property"))
    val className = if (sourceType.contains(".")) sourceType
    else
      getClass.getPackage.getName + "." + sourceType.head.toUpper + sourceType.substring(1) + "Source"
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[Source]
  }
}
