package com.github.andr83.parsek.spark.streaming

import com.github.andr83.parsek.PipeContext
import com.github.andr83.parsek.spark.sink.Sink
import com.github.andr83.parsek.spark.streaming.pipe.DStreamPipe
import com.github.andr83.parsek.spark.streaming.source.StreamingSource
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._


/**
  * @author andr83
  */
object ParsekStreamingJob extends StreamingJob {
  val DefaultFlow = "default"

  override def job(): Unit = {
    val sourcesByFlow = config.as[List[Config]]("sources")
      .groupBy(_.as[Option[String]]("flow") getOrElse DefaultFlow)
      .mapValues(_.map(StreamingSource.apply))

//    var streamsByFlow: Map[String, (PipeContext, DStream[PValue])] = sourcesByFlow.mapValues(sources => {
//      val streams = sources.map(_ (this))
//      val stream = streams.tail.foldRight(streams.head)(_.union(_))
//
//      val pipeContext = SparkPipeContext(ssc.sparkContext)
//      stream foreachRDD  (rdd=> {
//        pipeContext.getCounter(PipeContext.InfoGroup, PipeContext.InputRowsGroup) += rdd.count()
//      })
//      pipeContext -> stream
//    })
    val repository = new StreamFlowRepository(ssc.sparkContext)

    sourcesByFlow foreach {
      case (flow, sources)=>
        val streams = sources.map(_ (this))
        val stream = streams.tail.foldRight(streams.head)(_.union(_))

        val pipeContext = repository.getContext(flow)

        stream foreachRDD  (rdd=> {
          pipeContext.getCounter(PipeContext.InfoGroup, PipeContext.InputRowsGroup) += rdd.count()
        })

        repository += (flow -> stream)
    }

    val pipeConfigs = config.as[Option[List[Config]]]("pipes") getOrElse List.empty[Config]

    nextPipe(pipeConfigs, repository)

    val sinkConfigs = config.as[List[Config]]("sinks") groupBy (_.as[Option[String]]("flow") getOrElse DefaultFlow)
    val sinkFlows = sinkConfigs.keySet

    repository.streams filterKeys sinkFlows.contains foreach {
      case (flow, stream) => stream.foreachRDD(rdd => {
        val sinks = sinkConfigs.get(flow).get map Sink.apply
        val cachedRdd = rdd.cache()

        val pipeContext = repository.getContext(flow)

        cachedRdd foreachPartition (it=> {
          pipeContext.getCounter(PipeContext.InfoGroup, PipeContext.OutputRowsGroup) += it.size
        })

        sinks.foreach(_.sink(cachedRdd))

        logger.info(s"Flow $flow counters:")
        pipeContext.getCounters.toSeq.sortWith(_._1.toString() < _._1.toString()) foreach { case (key, count) =>
          logger.info(s"$key: $count")
        }
      })
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def nextPipe(pipes: List[Config], repository: StreamFlowRepository):Unit = pipes match {
    case head :: tail =>
      runPipe(head, repository)
      nextPipe(tail, repository)
    case Nil =>
  }

  def runPipe(pipeConfig: Config, repository: StreamFlowRepository): Unit = {
    val pipe = DStreamPipe(pipeConfig)
    val flow = pipeConfig.as[Option[String]]("flow") getOrElse DefaultFlow
    pipe.run(flow, repository)
//    // doing in this ugly way because streams ++ pipe.run not work
//    streams.flatMap {
//      case m@(streamFlow, (streamContext, stream)) if streamFlow == flow =>
//        Map(m) ++ pipe.run(streamFlow, stream)(streamContext).mapValues(streamContext -> _)
//      case m => Map(m)
//    }
  }
}
