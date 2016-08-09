package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek.PValuePredicate
import com.github.andr83.parsek.spark.streaming.StreamFlowRepository
import com.github.andr83.parsek.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Split stream with 2 flows based on predicate
  *
  * @param predicateFactory factory function to return PValue => Boolean instance
  * @param toFlows Flows name to split
  * @author andr83
  */
case class SpanDStreamPipe(predicateFactory: () => PValuePredicate, toFlows: Seq[String]) extends DStreamPipe {

  def this(config: Config) = this(
    predicateFactory = () => RuntimeUtils.compileFilterFn(config.as[String]("predicate")),
    toFlows = config.as[Seq[String]]("toFlows")
  )

  assert(toFlows.size == 2)

  lazy val predicate: PValuePredicate = predicateFactory()

  override def run(flow: String, repository: StreamFlowRepository): Unit = {
    val spanned = repository.getStream(flow).map { r =>
      predicate(r) -> r
    }.cache()

    val left = spanned.filter(_._1).map(_._2)
    val right = spanned.filter(!_._1).map(_._2)

    repository.getContext(toFlows.head, flow)
    repository.getContext(toFlows.last, flow)

    repository += (toFlows.head -> left)
    repository += (toFlows.last -> right)
  }
}
