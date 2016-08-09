package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.util.RuntimeUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * @author andr83 
  *         created on 05.08.16
  */
case class UpdatePipe(
  fn: () => String => PValue,
  field: FieldPath,
  as: Option[FieldPath] = None
) extends TransformPipe(field, as) {

  def this(config: Config) = this(
    fn = () => RuntimeUtils.compileTransformStringFn(config.as[String]("fn")),
    field = config.as[Option[String]]("field").getOrElse("").asFieldPath,
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  val transformFn = fn()
  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    Some(transformFn(str))
  }
}
