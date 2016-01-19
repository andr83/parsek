package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.util.matching.Regex

/**
  * @author andr83
  */
case class ReplacePipe(
  regex: Regex,
  replacement: String,
  field: FieldPath,
  as: Option[FieldPath] = None) extends TransformPipe(field, as)
{
  def this(config: Config) = this(
    regex = config.as[String]("regex").r,
    replacement = config.as[Option[String]]("with").getOrElse(""),
    field = config.as[Option[String]]("field").getOrElse("").asFieldPath,
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    Some(regex.replaceAllIn(str, replacement))
  }
}
