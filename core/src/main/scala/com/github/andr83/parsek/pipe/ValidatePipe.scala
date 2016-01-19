package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.util.{Failure, Success, Try}


/**
 * @author andr83
 */
case class ValidatePipe(root: RecordField) extends Pipe {

  def this(config: Config) {
    this(ValidatePipe.recordFieldFromConfig(config))
  }

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = {
    Try(root.validate(value)) match {
      case Success(validated) => validated
      case Failure(RequiredFieldError(f, ex)) =>
        logger.error(ex.toString, ex)
        context.getCounter(PipeContext.ErrorGroup, (ex.getClass.getSimpleName, f.name).toString()) += 1
        None
      case Failure(error) =>
        logger.error(error.toString, error)
        context.getCounter(PipeContext.ErrorGroup, error.getClass.getSimpleName) += 1
        None
    }
  }
}

object ValidatePipe {
  def apply(config: Config): ValidatePipe = new ValidatePipe(config)

  def recordFieldFromConfig(config: Config): RecordField = {
    RecordField(name = "root", fields = config.as[List[Config]]("fields") map Field.apply)
  }
}