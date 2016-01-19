package com.github.andr83.parsek.spark.pipe

import com.github.andr83.parsek.spark.FlowRepository
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * @author andr83
  */
case class RepartitionRDDPipe(numPartitions: Int) extends RDDPipe{

  def this(config: Config) = this(
    numPartitions = config.as[Int]("numPartitions")
  )
  /**
    * Transform flow
    *
    * @param flow flow name
    * @param repository FlowRepository
    * @return
    */
  override def run(flow: String, repository: FlowRepository): Unit = {
    val rdd = repository.getRdd(flow)
    repository += (flow -> rdd.repartition(numPartitions))
  }
}
