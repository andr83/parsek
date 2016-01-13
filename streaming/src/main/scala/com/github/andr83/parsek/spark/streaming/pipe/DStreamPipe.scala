package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek.{PValue, PipeContext}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.dstream.DStream

/**
  * DStreamPipe allow to transform current flow to one or multiple flows
  *
  * @author andr83
  */
trait DStreamPipe extends LazyLogging {

  /**
    * Transfoem flow
    *
    * @param flow flow name
    * @param stream flow stream
    * @param pipeContext
    * @return
    */
  def run(flow: String, stream: DStream[PValue])(implicit pipeContext: PipeContext): Map[String, DStream[PValue]]
}

object DStreamPipe {
  def apply(config: Config): DStreamPipe = {
    val pipeName = config.as[Option[String]]("type").getOrElse("parsek").split(":").head
    val className = if (pipeName.contains(".")) pipeName else
      getClass.getPackage.getName + "." + pipeName.head.toUpper + pipeName.substring(1) + "DStreamPipe"
    Class.forName(className).getConstructor(classOf[Config]).newInstance(config).asInstanceOf[DStreamPipe]
  }
}
