package com.github.andr83.parsek.spark.streaming.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import org.apache.spark.streaming.dstream.DStream

/**
  * DStreamPipe use parsek core library pipes to transform PValue's
  *
  * @param pipeConfig Configuration object for parsek pipe
  *
  * @author andr83
  */
case class ParsekDStreamPipe(pipeConfig: Config) extends DStreamPipe {
  override def run(flow: String, stream: DStream[PValue])(implicit pipeContext: PipeContext): Map[String, DStream[PValue]] = {
    val typeParts = pipeConfig.getString("type").split(":")
    assert(typeParts.length == 2)

    val pipeline = new Pipeline(Pipe(Map("type" -> typeParts.last).withFallback(pipeConfig)))
    Map(flow -> stream.flatMap(pipeline.run))
  }
}
