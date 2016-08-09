package com.github.andr83.parsek.spark.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.FlowRepository
import com.github.andr83.parsek.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.util.{Failure, Success, Try}

/**
  * Filter RDD by predicate function PValue => Boolean
  *
  * @param predicateFactory factory function to return PValue => Boolean instance
  * @param toFlow New flow name after filtering, by default it's current flow
  * @author andr83
  */
case class FilterRDDPipe (predicateFactory: () => PValuePredicate, toFlow: Option[String] = None) extends RDDPipe {

  def this(config: Config) = this(
    predicateFactory = () => RuntimeUtils.compileFilterFn(config.as[String]("predicate")),
    toFlow = config.as[Option[String]]("toFlow")
  )

  lazy val predicate = predicateFactory()

  override def run(flow: String, repository: FlowRepository):Unit = {
    val cachedRdd = repository.getRdd(flow).cache()

    val context = repository.getContext(toFlow.getOrElse(flow), flow)

    repository += (toFlow.getOrElse(flow) -> cachedRdd.filter(r => Try(predicate(r)) match {
      case Success(res) => res
      case Failure(error) =>
        logger.error(error.toString)
        context.getCounter(PipeContext.ErrorGroup, error.getClass.getSimpleName) += 1
        false
    }))
  }
}
