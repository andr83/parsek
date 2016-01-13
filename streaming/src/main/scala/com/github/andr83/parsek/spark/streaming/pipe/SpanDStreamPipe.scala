package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek.spark.util.RuntimeUtils
import com.github.andr83.parsek.{PValue, PValuePredicate, PipeContext}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.dstream.DStream

/**
  * Split stream with 2 flows based on predicate
  *
  * @param predicateFactory factory function to return PValue => Boolean instance
  * @param toFlows Flows name to split
  *
  * @author andr83
  */
case class SpanDStreamPipe(predicateFactory: () => PValuePredicate, toFlows: Seq[String]) extends DStreamPipe {

  def this(config: Config) = this(
    predicateFactory = () => RuntimeUtils.compileFilterFn(config.as[String]("predicate")),
    toFlows = config.as[Seq[String]]("toFlows")
  )

  assert(toFlows.size == 2)

  lazy val predicate: PValuePredicate = predicateFactory()

  override def run(flow: String, stream: DStream[PValue])(implicit pipeContext: PipeContext): Map[String, DStream[PValue]] = {
    Map(
      toFlows.head -> stream.filter(r => predicate(r)),
      toFlows.last -> stream.filter(r => !predicate(r))
    )
  }
}
