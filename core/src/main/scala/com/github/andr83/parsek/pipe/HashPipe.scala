package com.github.andr83.parsek.pipe

import java.security.MessageDigest

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.commons.codec.binary.Hex

/**
  * @author andr83
  */
case class HashPipe(
  algorithm: String,
  saltBefore: Option[String],
  saltAfter: Option[String],
  field: FieldPath,
  as: Option[FieldPath] = None
) extends TransformPipe(field, as) {

  def this(config: Config) = this(
    algorithm = config.as[String]("algorithm"),
    saltBefore = config.as[Option[String]]("saltBefore"),
    saltAfter = config.as[Option[String]]("saltAfter"),
    field = config.as[Option[String]]("field").getOrElse("").asFieldPath,
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  val digest = MessageDigest.getInstance(algorithm)

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    var value = str
    saltBefore foreach (salt=> value = salt + value)
    saltAfter  foreach (salt=> value = value + salt)
    val bytes = value.asBytes

    digest.reset()
    digest.update(bytes)
    val hash: Array[Byte] = digest.digest
    Some(PString(new String(Hex.encodeHex(hash))))
  }
}
