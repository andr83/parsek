package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek.spark.streaming.StreamFlowRepository
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._

/**
  * DStreamPipe allow to transform current flow to one or multiple flows
  *
  * @author andr83
  */
trait DStreamPipe extends LazyLogging {

  /**
    * Transform flow
    *
    * @param flow flow name
    * @param stream flow stream
    * @param pipeContext implicit context for parsek jobs
    * @return
    */
  def run(flow: String, repository: StreamFlowRepository): Unit
}

object DStreamPipe {
  def apply(config: Config): DStreamPipe = {
    val typeName = config
      .as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("DStreamPipe config must have type property"))
    val pipeName = typeName.split(":").head
    val className = if (pipeName.contains(".")) pipeName
    else
      getClass.getPackage.getName + "." + pipeName.head.toUpper + pipeName.substring(1) + "DStreamPipe"
    Class.forName(className).getConstructor(classOf[Config]).newInstance(config).asInstanceOf[DStreamPipe]
  }
}
