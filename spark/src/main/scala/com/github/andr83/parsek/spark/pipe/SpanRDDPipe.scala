package com.github.andr83.parsek.spark.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.FlowRepository
import com.github.andr83.parsek.spark.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.util.{Failure, Success, Try}

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
    val cachedRdd = repository.getRdd(flow).cache()

    val firstContext = repository.getContext(toFlows.head, flow)
    val secondContext = repository.getContext(toFlows.last, flow)

    repository += (toFlows.head -> cachedRdd.filter(r => Try(predicate(r)) match {
      case Success(res) => res
      case Failure(error) =>
        logger.error(error.toString)
        firstContext.getCounter(PipeContext.ErrorGroup, error.getClass.getSimpleName) += 1
        false
    }))

    repository += (toFlows.last -> cachedRdd.filter(r =>Try(predicate(r)) match {
      case Success(res) => !res
      case Failure(error) =>
        logger.error(error.toString)
        secondContext.getCounter(PipeContext.ErrorGroup, error.getClass.getSimpleName) += 1
        false
    }))
  }
}
