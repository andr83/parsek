package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.commons.codec.binary.Base64

/**
  * Decode string value as base64
  *
  * @param field field path to decode
  * @param as path where to save result of decoding
  *
  * @author andr83
  */
case class DecodeBase64Pipe(field: FieldPath, as: Option[FieldPath] = None) extends TransformPipe(field, as) {

  def this(config: Config) =
    this(config.as[String]("field").asFieldPath, config.as[Option[String]]("as").map(_.asFieldPath))

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = Some(PString(
    Base64.decodeBase64(str.asBytes).asStr
  ))
}

object DecodeBase64Pipe {
  def apply(config: Config): DecodeBase64Pipe = new DecodeBase64Pipe(config)
}
