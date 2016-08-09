package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.streaming.StreamFlowRepository
import com.github.andr83.parsek.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.util.{Failure, Success, Try}

/**
  * Filter stream by predicate function PValue => Boolean
  *
  * @param predicateFactory factory function to return PValue => Boolean instance
  * @param toFlow New flow name after filtering, by default it's current flow
  * @author andr83
  */
case class FilterDStreamPipe(predicateFactory: () => PValuePredicate, toFlow: Option[String] = None) extends DStreamPipe {

  def this(config: Config) = this(
    predicateFactory = () => RuntimeUtils.compileFilterFn(config.as[String]("predicate")),
    toFlow = config.as[Option[String]]("toFlow")
  )

  lazy val predicate = predicateFactory()

  override def run(flow: String, repository: StreamFlowRepository): Unit = {
    val cachedStream = repository.getStream(flow).cache()

    val context = repository.getContext(toFlow.getOrElse(flow), flow)

    repository += (toFlow.getOrElse(flow) -> cachedStream.filter(r => Try(predicate(r)) match {
      case Success(res) => res
      case Failure(error) =>
        logger.error(error.toString)
        context.getCounter(PipeContext.ErrorGroup, error.getClass.getSimpleName) += 1
        false
    }))
  }
}