package com.github.andr83.parsek.spark.streaming

import com.github.andr83.parsek.spark.SparkPipeContext
import com.github.andr83.parsek.{PValue, PipeContext}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Repository to get access to streams and their contexts in flow
  *
  * @param sc SparkContext to create SparkPipeContext instances
  *
  * @author andr83
  */
class StreamFlowRepository(sc: SparkContext) {
  protected var streamByFlow: Map[String, DStream[PValue]] = Map.empty[String, DStream[PValue]]
  protected var contextByFlow: Map[String, PipeContext] = Map.empty[String, PipeContext]

  def streams = streamByFlow

  /**
    * Return PipeContext for current flow. If context is not available it will created
    *
    * @param flow flow name
    * @return
    */
  def getContext(flow: String): PipeContext = getContext(flow, flow)

  /**
    * Return PipeContext for current flow. If context is not available it will copied from currentFlow or created
    *
    * @param flow flow name for which return PipeContext
    * @param currentFlow flow which use to create PipeContext if it is not exist
    *
    * @return
    */
  def getContext(flow: String, currentFlow: String): PipeContext = contextByFlow.getOrElse(flow, {
    val context = SparkPipeContext(sc)
    contextByFlow = contextByFlow + (flow -> context)

    if (currentFlow != flow && contextByFlow.contains(currentFlow)) {
      contextByFlow.get(currentFlow).get.getCounters foreach {
        case ((groupName, name), count) => context.getCounter(groupName, name) += count
      }
    }

    context
  })

  /**
    * Return stream for flow. If stream is not available exeption will thrown
    *
    * @param flow flow name
    * @return
    */
  def getStream(flow: String): DStream[PValue] = streamByFlow.getOrElse(flow,
    throw new IllegalStateException(s"Flow $flow is unavailable. Please check configuration."))

  /**
    * Assign stream to flow
    *
    * @param flowStream
    */
  def +=(flowStream: (String, DStream[PValue])) = streamByFlow = streamByFlow + flowStream
}
