package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.Pipe
import com.github.andr83.parsek.spark.streaming.StreamFlowRepository
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * DStreamPipe use parsek core library pipes to transform PValue's
  *
  * @param pipeConfig Configuration object for parsek pipe
  * @author andr83
  */
case class ParsekDStreamPipe(pipeConfig: Config, toFlow: Option[String]) extends DStreamPipe {

  def this(config: Config) = this(
    pipeConfig = config,
    toFlow = config.as[Option[String]]("toFlow")
  )

  override def run(flow: String, repository: StreamFlowRepository): Unit = {
    val stream = repository.getStream(flow)

    val res = stream.transform(rdd => {
      implicit val context = repository.getContext(toFlow.getOrElse(flow), flow)
      rdd.mapPartitions(it => {
        val pipeline = if (pipeConfig.hasPath("pipe")) {
          val pipeType = pipeConfig.as[String]("pipe")
          new Pipeline(Pipe(Map("type" -> pipeType).withFallback(pipeConfig)))
        } else {
          val pipeConfigs = pipeConfig.as[Seq[Config]]("pipes")
          Pipeline(pipeConfigs)
        }
        it.flatMap(pipeline.run)
      })
    })
    repository += (toFlow.getOrElse(flow) -> res)
  }
}
