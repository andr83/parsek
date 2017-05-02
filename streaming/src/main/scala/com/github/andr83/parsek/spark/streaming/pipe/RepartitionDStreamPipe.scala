package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek.spark.streaming.StreamFlowRepository
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * @author andr83 
  *         created on 20.03.17
  */
case class RepartitionDStreamPipe(numPartitions: Int) extends DStreamPipe {

  def this(config: Config) = this(numPartitions = config.as[Int]("numPartitions"))

  override def run(flow: String, repository: StreamFlowRepository): Unit = {
    val stream = repository.getStream(flow)

    val res = stream.repartition(numPartitions)
    repository += (flow -> res)
  }
}
