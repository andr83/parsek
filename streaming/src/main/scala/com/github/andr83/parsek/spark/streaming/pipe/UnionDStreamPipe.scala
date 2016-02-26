package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek.spark.streaming.StreamFlowRepository
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * @author andr83
  */
case class UnionDStreamPipe(flows: Seq[String], toFlow: String) extends DStreamPipe {

  def this(config: Config) = this(
    flows = config.as[Seq[String]]("flows"),
    toFlow = config.as[Option[String]]("toFlow") getOrElse "default"
  )

  override def run(flow: String, repository: StreamFlowRepository) = {
    val streams = flows map repository.getStream
    val stream = streams.tail.foldRight(streams.head)(_.union(_))
    repository += toFlow -> stream
  }
}
