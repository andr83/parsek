package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.github.andr83.parsek.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.util.{Failure, Success, Try}

/**
  * Created by bfattahov on 11/07/2017.
  */
case class MapPipe(mapFactory: () => PMap => PValue) extends Pipe {
  def this(config: Config) = this(
    mapFactory = () => RuntimeUtils.compileTransformFromTo[PMap, PValue](config.as[String]("map"))
  )

    private val mapFunction: PMap => PValue = mapFactory()

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = {
    value match {
      case x: PMap => val result = Try(mapFunction(x))
        result match {
          case Success(map) => Some(map)
          case Failure(error) => logger.error(error.toString, error)
            context.getCounter(PipeContext.ErrorGroup, error.getClass.getSimpleName) += 1
            None
        }
      case _ => throw IllegalValueType(s"MapPipe expect map value type but got $value")
    }
  }
}

object MapPipe {

  def apply(config: Config): MapPipe = new MapPipe(config)
}