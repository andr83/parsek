package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


/**
 * @author andr83
 */
case class Fields(root: MapField) extends Pipe {

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = {
    implicit val errors = mutable.ListBuffer.empty[FieldError]
    Try(root.validate(value)) match {
      case Success(validated) =>
        if (errors.nonEmpty) {
          errors.foreach{
            case (f, ex)=>
              context.getCounter(PipeContext.WarnGroup, (ex.getClass.getSimpleName, f.name).toString()) += 1
          }
        }
        validated
      case Failure(error) =>
        error match {
          case RequiredFieldError(f, ex) =>
            logger.error(ex.toString, ex)
            context.getCounter(PipeContext.ErrorGroup, (ex.getClass.getSimpleName, f.name).toString()) += 1
          case _ =>
            logger.error(error.toString, error)
            context.getCounter(PipeContext.ErrorGroup, error.getClass.getSimpleName) += 1
        }
        None
    }
  }
}

object Fields {
  def apply(config: Config): Fields = {
    val fields = config.as[List[Config]]("config") map Field.apply
    val root = MapField(name = "root", fields = Some(fields))
    Fields(root)
  }
}