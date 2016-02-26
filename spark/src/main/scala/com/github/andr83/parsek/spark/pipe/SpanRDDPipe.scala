package com.github.andr83.parsek.spark.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.FlowRepository
import com.github.andr83.parsek.spark.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Split RDD with 2 flows based on predicate
  *
  * @param predicateFactory factory function to return PValue => Boolean instance
  * @param toFlows New flows names
  *
  * @author andr83
  */
case class SpanRDDPipe(predicateFactory: () => PValuePredicate, toFlows: Seq[String]) extends RDDPipe {

  def this(config: Config) = this(
    predicateFactory = () => RuntimeUtils.compileFilterFn(config.as[String]("predicate")),
    toFlows = config.as[Seq[String]]("toFlows")
  )

  assert(toFlows.size == 2)

  lazy val predicate: PValuePredicate = predicateFactory()

  override def run(flow: String, repository: FlowRepository):Unit = {
    val spanned = repository.getRdd(flow).map { r =>
      predicate(r) -> r
    }.cache()

    val left = spanned.filter(_._1).values
    val right = spanned.filter(!_._1).values

    repository.getContext(toFlows.head, flow)
    repository.getContext(toFlows.last, flow)

    repository += (toFlows.head -> left)
    repository += (toFlows.last -> right)
  }
}
