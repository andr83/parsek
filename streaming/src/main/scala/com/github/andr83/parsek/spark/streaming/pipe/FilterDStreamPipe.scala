package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.dstream.DStream

/**
  * Filter stream by predicate function PValue => Boolean
  *
  * @param predicateFactory factory function to return PValue => Boolean instance
  * @param toFlow New flow name after filtering, by default it's current flow
  *
  * @author andr83
  */
case class FilterDStreamPipe(predicateFactory: () => PValuePredicate, toFlow: Option[String] = None) extends DStreamPipe {

  def this(config: Config) = this(
    predicateFactory = () => RuntimeUtils.compileFilterFn(config.as[String]("predicate")),
    toFlow = config.as[Option[String]]("toFlow")
  )

  lazy val predicate = predicateFactory()

  override def run(flow: String, stream: DStream[PValue])(implicit pipeContext: PipeContext): Map[String, DStream[PValue]] = {
    Map(toFlow.getOrElse(flow) -> stream.filter(r=>predicate(r)))
  }
}