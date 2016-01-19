package com.github.andr83.parsek.spark.pipe

import com.github.andr83.parsek.spark.FlowRepository
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._

/**
  * DStreamPipe allow to transform current flow to one or multiple flows
  *
  * @author andr83
  */
trait RDDPipe extends LazyLogging {

  /**
    * Transfoem flow
    *
    * @param flow flow name
    * @param repository FlowRepository
    * @return
    */
  def run(flow: String, repository: FlowRepository): Unit
}

object RDDPipe {
  def apply(config: Config): RDDPipe = {
    val typeName = config
      .as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("RDDPipe config must have type property"))
    val pipeName = typeName.split(":").head
    val className = if (pipeName.contains(".")) pipeName else
      getClass.getPackage.getName + "." + pipeName.head.toUpper + pipeName.substring(1) + "RDDPipe"
    Class.forName(className).getConstructor(classOf[Config]).newInstance(config).asInstanceOf[RDDPipe]
  }
}