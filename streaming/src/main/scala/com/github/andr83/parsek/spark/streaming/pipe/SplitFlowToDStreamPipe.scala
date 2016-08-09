package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek.PValue
import com.github.andr83.parsek.spark.streaming.StreamFlowRepository
import com.github.andr83.parsek.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Split stream to flows based on (PValue=>String) function
  *
  * @param splitFnFactory factory function to return (PValue=>String) instance
  * @param toFlows Flows name to split
  * @author andr83
  */
case class SplitFlowToDStreamPipe(splitFnFactory: ()=> (PValue=>String), toFlows: Seq[String]) extends DStreamPipe{

  def this(config: Config) = this(
    splitFnFactory = () => RuntimeUtils.compileTransformFn[PValue=>String](config.as[String]("splitFn")),
    toFlows = config.as[Seq[String]]("toFlows")
  )

  assert(toFlows.nonEmpty)

  lazy val splitFn = splitFnFactory()

  override def run(flow: String, repository: StreamFlowRepository): Unit = {
    val grouped = repository.getStream(flow).map { r =>
      splitFn(r) -> r
    } cache()

    toFlows foreach (flow => {
      repository += (flow -> grouped.filter(_._1 == flow).map(_._2))
    })
  }
}
