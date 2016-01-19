package com.github.andr83.parsek.spark.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.Pipe
import com.github.andr83.parsek.spark.FlowRepository
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * ParsekRDDPipe use parsek core library pipes to transform RDD PValue's
  * @author andr83
  */
case class ParsekRDDPipe(pipeConfig: Config, toFlow: Option[String]) extends RDDPipe {
  def this(config: Config) = this(
    pipeConfig = config,
    toFlow = config.as[Option[String]]("toFlow")
  )

  override def run(flow: String, repository: FlowRepository):Unit = {
    val pipeType = pipeConfig.as[String]("pipe")
    val rdd = repository.getRdd(flow)
    implicit val context = repository.getContext(toFlow.getOrElse(flow), flow)

    repository += (toFlow.getOrElse(flow) -> rdd.mapPartitions(it=> {
      val pipeline = new Pipeline(Pipe(Map("type" -> pipeType).withFallback(pipeConfig)))
      it.flatMap(pipeline.run)
    }))
  }
}
