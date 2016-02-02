package com.github.andr83.parsek.japi

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.Pipe
import com.typesafe.config.Config

import scala.collection.JavaConversions._

/**
  * @author Andrei Tupitcyn
  */
class JavaPipeline(pipes: Pipe*) {

  private val pipeline = new Pipeline(pipes.toSeq: _*)

  def run(value: PValue, context: PipeContext): java.util.List[PValue] = {
    pipeline.run(value)(context)
  }
}

object JavaPipeline {
  def fromConfigList(configList: java.util.List[_ <: Config]): JavaPipeline = {
    new JavaPipeline(configList.map(Pipe.apply): _*)
  }
}